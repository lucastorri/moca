package com.github.lucastorri.moca.store.control

import java.util.concurrent.Semaphore

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.store.serialization.SerializerService
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.{DB, DBMaker}

import scala.async.Async._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

class PgRunControl(
  config: Config,
  serializers: SerializerService,
  partition: PartitionSelector,
  implicit val ec: ExecutionContext
) extends RunControl with PgRunControlSchema with StrictLogging { self =>

  import PgDriver.api._

  protected val db = Database.forConfig("connection", config)
  protected val ws = serializers.create[CriteriaHolder]
  protected val ts = serializers.create[ContentLinksTransferHolder]
  private val running = new RunSet

  private def init(): Future[Unit] = async {
    await(db.run(DBIO.seq(
      createWorkTable,
      createRunTable,
      createTaskTable,
      createUrlDepthTable,
      createOutstandingUrlTable,
      createPartialResultTable,
      createFinalResultTable
    ).transactionally))

    val loaded = await(db.run(selectAllRuns)).map { run =>
      val selectData =
        for {
          idsAndPartitions <- selectTaskIdsAndPartitions(run.id)
          depths <- selectUrlDepths(run.id)
        } yield (idsAndPartitions, depths)
      db.run(selectData).map(data => run -> data)
    }

    await(Future.sequence(loaded)).foreach { case (run, (idsAndPartitions, depths)) =>
      idsAndPartitions.foreach { case (taskId, part) =>
        run.addTask(part, taskId)
        run.depths.project(depths).commit()
      }
      running.add(run)
    }
  }

  Await.result(init(), config.getDuration("init-timeout").toMillis.millis)


  override def add(works: Set[Work]): Future[Unit] = {
    val newRuns = mutable.ListBuffer.empty[(Run, Task)]
    val inserts = works.toSeq.flatMap { work =>
      if (running.hasWork(work.id)) {
        logger.trace(s"Skipping start of already running work ${work.id}")
        Seq.empty
      } else {
        val run = Run(Id.newRunId, work.id, work.criteria)
        val task = Task(Id.newTaskId(run.id), Set(work.seed), work.criteria, 0, partition(work.seed))
        newRuns.append(run -> task)
        Seq(insertWork(work), insertRun(run), insertTask(run.id, task), insertUrlDepth(run.id, work.seed, 0))
      }
    }

    db.run(DBIO.sequence(inserts).transactionally).map { _ =>
      val tasks = newRuns.map { case (run, task) =>
        running.add(run)
        run.addTask(task.partition, task.id)
        task
      }
      publish(tasks.toSet)
    }
  }

  override def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit] = withRun(parentTaskId) { run =>
    val newTasks = mutable.ListBuffer.empty[Task]

    val currentDepths = run.depths

    val depthStatus = urls.map(url => (url, partition(url)) -> depth).toMap
      .groupBy { case ((url, part), d) => currentDepths.depthStatus(url, part, d) }
      .withDefaultValue(Map.empty)

    val betterUrls = (depthStatus(NewDepth) ++ depthStatus(SmallerDepth))
      .map { case ((url, _), _) => url }
      .toSet

    val usedUrls = mutable.HashMap.empty[(Url, String), Int]

    val updates = betterUrls.groupBy(partition.apply).flatMap { case (part, group) =>
      if (run.hasPartition(part)) {
        group.map(url => insertOutstandingUrl(run.id, url, depth, part))
      } else {
        val task = Task(Id.newTaskId(run.id), group, run.criteria, depth, part)
        newTasks.append(task)
        insertTask(run.id, task) +: group.map { url =>
          usedUrls.put(url -> part, depth)
          if (depthStatus(NewDepth).contains(url -> part)) insertUrlDepth(run.id, url, depth)
          else updateUrlDepth(run.id, url, depth)
        }.toSeq
      }
    }

    db.run(DBIO.sequence(updates).transactionally).map { _ =>
      currentDepths.project(usedUrls.toMap).commit()
      publish(run, newTasks.toSet)
    }
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = withRun(taskId) { run =>

    val part = run.partition(taskId)
    val currentDepths = run.depths
    val projectedDepths = currentDepths.project(transfer)

    async {

      val outstanding = await(db.run(selectOutstandingUrls(run.id, run.partition(taskId))))
      val depthStatus = outstanding
        .groupBy { case (url, depth) => projectedDepths.depthStatus(url, part, depth) }
        .withDefaultValue(Seq.empty)

      val better = depthStatus(NewDepth) ++ depthStatus(SmallerDepth)
      val worse = depthStatus(IgnoredDepth)

      val outstandingToDelete = mutable.HashSet.empty[(Url, Int)] ++ worse

      val outstandingCandidates = better.groupBy { case (url, _) => url }
        .values
        .map { urls =>
        val sorted = urls.distinct.sortBy { case (_, depth) => depth }
        outstandingToDelete ++= sorted.tail
        sorted.head
      }

      val hasTasksLeft = run.hasOtherTasks || outstandingCandidates.nonEmpty

      val update =
        if (hasTasksLeft) {
          val taskToInsert =
            if (outstandingCandidates.isEmpty) {
              None
            } else {
              val (_, shallowestDepth) = outstandingCandidates.minBy { case (_, depth) => depth }
              val newTaskSeeds = outstandingCandidates
                .filter { case (_, depth) => depth == shallowestDepth }
                .map { case (url, _) => url }
                .toSet
              outstandingToDelete ++= newTaskSeeds.map(url => url -> shallowestDepth)
              Some(Task(Id.newTaskId(run.id), newTaskSeeds, run.criteria, shallowestDepth, part))
            }

          val updates =
            outstandingToDelete.toSeq.map { case (url, depth) => deleteOutstandingUrl(run.id, url, depth, part) } ++
            depthStatus(NewDepth).map { case (url, depth) => insertUrlDepth(run.id, url, depth) } ++
            depthStatus(SmallerDepth).map { case (url, depth) => updateUrlDepth(run.id, url, depth) } ++
            taskToInsert.map(task => insertTask(run.id, task)) :+
            insertPartialResult(run.id, transfer) :+
            deleteTask(run.id, taskId)

          db.run(DBIO.sequence(updates).transactionally).map { _ =>
            run.finishTask(taskId)
            publish(run, taskToInsert.toSet)
            projectedDepths.commit()
            None
          }
        } else {
          val finalTransfer = await {
            db.run(selectPartialResults(run.id)).map { transfers =>
              CombinedLinksTransfer(transfers.toArray :+ transfer)
            }
          }

          val updates = Seq(
            deleteOutstandingUrl(run.id),
            deletePartialResults(run.id),
            deleteTasks(run.id),
            deleteUrlDepths(run.id),
            deleteRun(run.id),
            insertFinalResult(run.workId, finalTransfer))

          db.run(DBIO.sequence(updates).transactionally).map { _ =>
            running.remove(run)
            Some(run.workId)
          }
        }

      await(update)
    }
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = {
    db.run(selectFinalResult(workId))
  }

  override def abort(taskIds: Set[String]): Future[Unit] = {
    val selects = taskIds.map { taskId =>
      withRun(taskId) { run =>
        db.run(selectTask(taskId, run.criteria))
      }
    }
    Future.sequence(selects).map { results =>
      publish(results.flatten)
    }
  }

  override def close(): Unit = {
    db.close()
  }

  private def publish(run: Run, tasks: Set[Task]): Unit = {
    tasks.foreach(task => run.addTask(task.partition, task.id))
    publish(tasks)
  }

  private def withRun[R](taskId: String)(f: Run => Future[R]): Future[R] = {
    val runId = Id.idFor(taskId)
    running.get(runId) match {
      case Some(run) => run.exec(f(run))
      case None => Future.failed(new RuntimeException(s"Run $runId not found"))
    }
  }


  case class Run(id: String, workId: String, criteria: LinkSelectionCriteria) {

    private val lock = new Semaphore(1, true)
    private val partitionTaskId = mutable.HashMap.empty[String, String]
    private val taskIdPartition = mutable.HashMap.empty[String, String]
    private val fdb = DBMaker
      .tempFileDB()
      .deleteFilesAfterClose()
      .closeOnJvmShutdown()
      .fileMmapEnableIfSupported()
      .cacheLRUEnable()
      .make()
    private val knownDepths = fdb.hashMap[Long, Int]("known-depths")

    def depths = new CurrentDepths(knownDepths, fdb)

    def addTask(partition: String, taskId: String): Unit = {
      partitionTaskId.put(partition, taskId)
      taskIdPartition.put(taskId, partition)
    }

    def finishTask(taskId: String): Unit =
      taskIdPartition.remove(taskId).foreach(partitionTaskId.remove)

    def hasPartition(partition: String): Boolean =
      partitionTaskId.contains(partition)

    def hasOtherTasks: Boolean =
      partitionTaskId.size > 1

    def partition(taskId: String): String =
      taskIdPartition(taskId)

    def exec[R](action: => Future[R]): Future[R] = {
      try {
        lock.acquire()
        val result = action
        result.onComplete { case _ => done() }
        result
      } catch { case e: Exception =>
        done()
        Future.failed(e)
      }
    }

    def close(): Unit = {
      fdb.rollback()
      fdb.close()
    }

    private def done(): Unit = {
      lock.release()
      if (!fdb.isClosed) {
        fdb.rollback()
      }
    }

  }

  class RunSet {

    private val workIds = mutable.HashSet.empty[String]
    private val running = mutable.HashMap.empty[String, Run]

    def add(run: Run): Unit = synchronized {
      running.put(run.id, run)
      workIds.add(run.workId)
    }

    def get(runId: String): Option[Run] =
      running.get(runId)

    def remove(run: Run): Unit = synchronized {
      running.remove(run.id).foreach(_.close())
      workIds.remove(run.workId)
    }

    def hasWork(workId: String): Boolean =
      workIds.contains(workId)

  }

  trait Depths {

    def get(key: Long): Option[Int]

    def depthStatus(url: Url, partition: String, depth: Int): DepthStatus = {
      get(depthKey(url, partition)) match {
        case Some(d) if depth < d => SmallerDepth
        case None => NewDepth
        case _ => IgnoredDepth
      }
    }

    def depthKey(url: Url, partition: String): Long =
      (partition.hashCode.toLong << 32) | (url.hashCode.toLong & 4294967295L)

  }

  class CurrentDepths(depths: java.util.Map[Long, Int], fdb: DB) extends Depths {

    override def get(key: Long): Option[Int] = Option(depths.get(key))

    def project(transfer: ContentLinksTransfer): ProjectedDepths = {
      transfer.contents.foreach(link => project(link.url, link.depth))
      new ProjectedDepths(depths, fdb)
    }

    def project(pairs: Seq[(Url, Int)]): ProjectedDepths = {
      pairs.foreach { case (url, depth) => project(url, depth) }
      new ProjectedDepths(depths, fdb)
    }

    def project(entries: Map[(Url, String), Int]): ProjectedDepths = {
      entries.foreach { case ((url, part), depth) => project(depthKey(url, part), depth) }
      new ProjectedDepths(depths, fdb)
    }

    private def project(url: Url, depth: Int): Unit =
      project(depthKey(url, partition(url)), depth)

    private def project(key: Long, depth: Int): Unit =
      depths.put(key, math.min(depth, depths.getOrElse(key, Int.MaxValue)))

  }

  class ProjectedDepths(depths: java.util.Map[Long, Int], fdb: DB) extends Depths {

    override def get(key: Long): Option[Int] = Option(depths.get(key))

    def commit(): Unit =
      fdb.commit()

  }

  sealed abstract class DepthStatus(val isBetter: Boolean, val isNew: Boolean)
  case object NewDepth extends DepthStatus(true, true)
  case object SmallerDepth extends DepthStatus(true, false)
  case object IgnoredDepth extends DepthStatus(false, false)

  case object Id {
    val separator = "::"
    def idFor(id: String): String = id.split(separator).head
    def newRunId = Random.alphanumeric.take(32).mkString
    def newTaskId(runId: String) = s"$runId$separator${Random.alphanumeric.take(16).mkString}"
  }

}

case class ContentLinksTransferHolder(transfer: ContentLinksTransfer)

case class CriteriaHolder(criteria: LinkSelectionCriteria)
