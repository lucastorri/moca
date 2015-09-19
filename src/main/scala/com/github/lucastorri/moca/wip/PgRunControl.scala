package com.github.lucastorri.moca.wip

import java.util.concurrent.Semaphore

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.serialization.SerializerService
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.async.Async.{async, await}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.Random

class PgRunControl(
  config: Config,
  serializers: SerializerService,
  publisher: TaskPublisher,
  partition: PartitionSelector,
  ec: ExecutionContext
) extends RunControl(publisher) with StrictLogging { self =>

  implicit val ec2 = ec

  import com.github.lucastorri.moca.store.work.PgDriver.api._

  private val db = Database.forConfig("connection", config)
  private val ws = serializers.create[CriteriaHolder]
  private val ts = serializers.create[ContentLinksTransferHolder]

  private val runningWorkIds = mutable.HashSet.empty[String]
  private val running = mutable.HashMap.empty[String, Run]

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
      db.run(selectTaskIdsAndPartitions(run.id)).map(idsAndPartitions => run -> idsAndPartitions)
    }

    await(Future.sequence(loaded)).foreach { case (run, idsAndPartitions) =>
      idsAndPartitions.foreach { case (taskId, part) =>
        run.addTask(part, taskId)
      }
      running.put(run.id, run)
      runningWorkIds.add(run.workId)
    }
  }

  Await.result(init(), config.getDuration("init-timeout").toMillis.millis)


  override def add(works: Set[Work]): Future[Unit] = { //TODO synchronized on whole future for `running`
    val newRuns = mutable.ListBuffer.empty[(Run, Task)]
    val inserts = works.toSeq.flatMap { work =>
      if (runningWorkIds.contains(work.id)) {
        logger.trace(s"Skipping start of already running work ${work.id}")
        Seq.empty
      } else {
        val run = Run(Id.newRunId, work.id, work.criteria)
        val task = Task(Id.newTaskId(run.id), Set(work.seed), work.criteria, 0, partition(work.seed))
        newRuns.append(run -> task)
        Seq(insertWork(work), insertRun(run), insertTask(run.id, task))
      }
    }

    db.run(DBIO.sequence(inserts).transactionally).map { _ =>
      val tasks = newRuns.map { case (run, task) =>
        running.put(run.id, run)
        run.addTask(task.partition, task.id)
        task
      }
      publish(tasks.toSet)
    }
  }

  override def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit] = withRun(parentTaskId) { run =>
    val newTasks = mutable.ListBuffer.empty[Task]

    val currentDepths = run.depths

    val urlsWithDepth = urls.map(url => (url, partition(url)) -> depth).toMap
    val betterUrls = urlsWithDepth
      .filter { case ((url, part), d) => currentDepths.depthStatus(url, part, d).isBetter }
      .map { case ((url, _), _) => url }
      .toSet

    val inserts = betterUrls.groupBy(partition.apply).flatMap { case (part, group) =>
      if (run.hasPartition(part)) {
        group.map(url => insertOutstandingUrl(run.id, url, depth, part))
      } else {
        val task = Task(Id.newTaskId(run.id), group, run.criteria, depth, part)
        newTasks.append(task)
        Seq(insertTask(run.id, task))
      }
    }

    db.run(DBIO.sequence(inserts).transactionally).map { _ =>
      currentDepths.project(urlsWithDepth).commit()
      publish(run, newTasks.toSet)
    }
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = withRun(taskId) { run =>

    val part = run.partition(taskId)
    val currentDepths = run.depths
    val newDepths = transfer.contents.map { link => (link.url, part) -> link.depth }.toMap
    val projectedDepths = currentDepths.project(newDepths)

    async {

      val outstanding = await(db.run(selectOutstandingUrls(run.id, run.partition(taskId))))
      val hasTasksLeft = run.hasOtherTasks || outstanding.nonEmpty

      val update =
        if (hasTasksLeft) {

          val depthStatus = outstanding
              .groupBy { case (url, depth) => projectedDepths.depthStatus(url, part, depth) }
              .withDefaultValue(Seq.empty)

          val better = depthStatus(NewDepth) ++ depthStatus(SmallerDepth)
          val worse = depthStatus(IgnoredDepth)

          val outstandingToDelete = mutable.HashSet.empty[(Url, Int)] ++ worse

          val candidates = better.groupBy { case (url, _) => url }
            .values
            .map { urls =>
              val sorted = urls.distinct.sortBy { case (_, depth) => depth }
              outstandingToDelete ++= sorted.tail
              sorted.head
            }

          val taskToInsert =
            if (candidates.isEmpty) {
              None
            } else {
              val (_, shallowestDepth) = candidates.minBy { case (_, depth) => depth }
              val newTaskSeeds = candidates
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
            runningWorkIds.remove(run.workId)
            Some(run.workId)
          }
        }

      await(update)
    }
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = {
    db.run(selectFinalResult(workId))
  }

  override def abort(taskId: String): Future[Unit] = withRun(taskId) { run =>
    async {
      await(db.run(selectTask(taskId, run.criteria))).foreach(task => publish(Set(task)))
    }
  }

  override def close(): Unit = {
    db.close()
  }

  private def publish(run: Run, tasks: Set[Task]): Unit = {
    tasks.foreach { task =>
      run.addTask(task.partition, task.id)
    }
    publish(tasks)
  }

  private def withRun[R](taskId: String)(f: Run => Future[R]): Future[R] = {
    val runId = Id.idFor(taskId)
    running.get(runId) match {
      case Some(run) => run.exec(f(run))
      case None => Future.failed(new RuntimeException(s"Run $runId not found"))
    }
  }

  private def createWorkTable: DBIO[Int] =
    sqlu"""
           create table if not exists work(
              id varchar(256) not null primary key,
              seed varchar(2048) not null,
              criteria bytea not null)
        """

  private def insertWork(work: Work) = {
    val serializedCriteria = ws.serialize(CriteriaHolder(work.criteria))
    sqlu"""insert into work (id, seed, criteria) values (${work.id}, ${work.seed.toString}, $serializedCriteria)"""
  }


  private def createUrlDepthTable: DBIO[Int] =
    sqlu"""
           create table if not exists url_depth(
              run_id varchar(128) not null,
              url varchar(1024) not null,
              depth integer not null,
              foreign key(run_id) references run(id),
              primary key(run_id, url))
        """

  private def insertUrlDepth(runId: String, url: Url, depth: Int): DBIO[Int] =
    sqlu"""insert into url_depth (run_id, url, depth) values ($runId, ${url.toString}, $depth)"""

  private def updateUrlDepth(runId: String, url: Url, depth: Int): DBIO[Int] =
    sqlu"""update url_depth set depth = $depth where run_id = $runId and url = ${url.toString}"""

  private def deleteUrlDepths(runId: String) = {
    sqlu"delete from url_depth where run_id = $runId"
  }


  private def createRunTable: DBIO[Int] =
    sqlu"""
           create table if not exists run(
              id varchar(128) not null primary key,
              work_id varchar(256) not null,
              criteria bytea not null,
              foreign key(work_id) references work(id))
        """

  private def insertRun(run: Run) = {
    val serializedCriteria = ws.serialize(CriteriaHolder(run.criteria))
    sqlu"insert into run (id, work_id, criteria) values (${run.id}, ${run.workId}, $serializedCriteria)"
  }

  private def deleteRun(runId: String) = {
    sqlu"delete from run where id = $runId"
  }

  private def selectAllRuns = {
    sql"select id, work_id, criteria from run".as[(String, String, Array[Byte])].map { results =>
      results.map { case (id, workId, serializedCriteria) =>
        Run(id, workId, ws.deserialize(serializedCriteria).criteria)
      }
    }
  }

  private def selectTaskIdsAndPartitions(runId: String) = {
    sql"select id, partition from task where run_id = run".as[(String, String)]
  }


  private def createOutstandingUrlTable: DBIO[Int] =
    sqlu"""
           create table if not exists outstanding_url(
              run_id varchar(128) not null,
              url varchar(1024) not null,
              depth integer not null,
              partition varchar(256) not null,
              foreign key(run_id) references run(id),
              primary key(run_id, url))
        """

  private def insertOutstandingUrl(runId: String, url: Url, depth: Int, partition: String) = {
    sqlu"insert into outstanding_url(run_id, url, depth, partition) values ($runId, ${url.toString}, $depth, $partition)"
  }

  private def deleteOutstandingUrl(runId: String, url: Url, depth: Int, partition: String) = {
    sqlu"delete from outstanding_url where run_id = $runId, url = ${url.toString}, depth = $depth, partition = $partition"
  }

  private def deleteOutstandingUrl(runId: String) = {
    sqlu"delete from outstanding_url where run_id = $runId"
  }

  private def selectOutstandingUrls(runId: String, partition: String) = {
    sql"select url, depth from outstanding_url where run_id = $runId and partition = $partition".as[(String, Int)]
      .map(_.map { case (url, depth) => Url(url) -> depth })
  }


  private def createFinalResultTable =
    sqlu"""
           create table if not exists final_result(
              id bigserial primary key,
              work_id varchar(256) not null,
              transfer bytea not null,
              foreign key(work_id) references work(id))
        """

  private def insertFinalResult(workId: String, transfer: ContentLinksTransfer) = {
    val serializedTransfer = ts.serialize(ContentLinksTransferHolder(transfer))
    sqlu"insert into final_result (work_id, transfer) values ($workId, $serializedTransfer)"
  }

  private def selectFinalResult(workId: String) = {
    sql"select transfer from final_result where work_id = $workId order by id desc".as[Array[Byte]]
      .headOption
      .map(_.map(bytes => ts.deserialize(bytes).transfer))
  }


  private def createPartialResultTable =
    sqlu"""
           create table if not exists partial_result(
              id bigserial primary key,
              run_id varchar(128) not null,
              transfer bytea not null,
              foreign key(run_id) references run(id))
        """

  private def insertPartialResult(runId: String, transfer: ContentLinksTransfer) = {
    val serializedTransfer = ts.serialize(ContentLinksTransferHolder(transfer))
    sqlu"insert into partial_result (run_id, transfer) values ($runId, $serializedTransfer)"
  }

  private def deletePartialResults(runId: String) = {
    sqlu"delete from partial_result where run_id = $runId"
  }

  private def selectPartialResults(runId: String) = {
    sql"select transfer from partial_result where run_id = $runId order by id asc".as[Array[Byte]]
      .map(_.map(bytes => ts.deserialize(bytes).transfer))
  }


  private def createTaskTable: DBIO[Int] =
    sqlu"""
           create table if not exists task(
              id varchar(256) primary key,
              run_id varchar(128) not null,
              seeds varchar(1024)[] not null,
              depth integer not null,
              partition varchar(256) not null,
              foreign key(run_id) references run(id))
        """

  private def insertTask(runId: String, task: Task) = {
    val seeds = task.seeds.map(_.toString).toSeq
    sqlu"""insert into task (id, run_id, seeds, depth, partition)
           values (${task.id}, $runId, $seeds, ${task.initialDepth}, ${task.partition})"""
  }

  private def selectTask(taskId: String, criteria: LinkSelectionCriteria) = {
    sql"select id, run_id, seeds, depth, partition where id = $taskId".as[(String, String, Seq[String], Int, String)]
      .headOption
      .map(_.map { case (id, runId, seeds, depth, part) => Task(id, seeds.toSet.map(Url.apply), criteria, depth, part) })
  }

  private def deleteTask(runId: String, taskId: String) = {
    sqlu"delete from task where id = $taskId and run_id = $runId"
  }

  private def deleteTasks(runId: String) = {
    sqlu"delete from task where run_id = $runId"
  }


  case class Run(id: String, workId: String, criteria: LinkSelectionCriteria) {

    private val lock = new Semaphore(1, true)
    private val partitionTaskId = mutable.HashMap.empty[String, String]
    private val taskIdPartition = mutable.HashMap.empty[String, String]
    private val knownDepths = mutable.HashMap.empty[(Int, Int), Int]

    def depths = new CurrentDepths(knownDepths)

    def addTask(partition: String, taskId: String): Unit = {
      partitionTaskId.put(partition, taskId)
      taskIdPartition.put(taskId, partition)
    }

    def finishTask(taskId: String): Unit = {
      taskIdPartition.remove(taskId).foreach(partitionTaskId.remove)
    }
    
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
        result.onComplete { case _ => lock.release() }
        result
      } catch { case e: Exception =>
        lock.release()
        Future.failed(e)
      }
    } 

  }

  trait Depths {
    def current: Map[(Int, Int), Int]

    def depthStatus(url: Url, partition: String, depth: Int): DepthStatus = {
      current.get(depthKey(url, partition)) match {
        case Some(d) if depth < d => SmallerDepth
        case None => NewDepth
        case _ => IgnoredDepth
      }
    }

    def depthKey(url: Url, partition: String): (Int, Int) = {
      partition.hashCode -> url.hashCode
    }

  }

  class CurrentDepths(depths: mutable.HashMap[(Int, Int), Int]) extends Depths {

    override val current = depths.toMap

    def project(other: Map[(Url, String), Int]): ProjectedDepths = new ProjectedDepths(depths, other)

  }

  class ProjectedDepths(depths: mutable.HashMap[(Int, Int), Int], other: Map[(Url, String), Int]) extends Depths {

    override val current = {
      val current = mutable.HashMap.empty[(Int, Int), Int] ++ depths
      other.foreach { case ((url, part), depth) =>
        val key = depthKey(url, part)
        val shallowest = math.min(current.getOrElse(key, Int.MaxValue), depth)
        current.put(key, shallowest)
      }
      current.toMap
    }

    def commit(): Unit = {
      depths ++= current
    }

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

case class CriteriaHolder(criteria: LinkSelectionCriteria)
case class ContentLinksTransferHolder(transfer: ContentLinksTransfer)

case class CombinedLinksTransfer(transfers: Seq[ContentLinksTransfer]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = transfers.toStream.flatMap(_.contents)

}