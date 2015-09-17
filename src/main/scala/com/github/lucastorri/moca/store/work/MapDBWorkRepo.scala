package com.github.lucastorri.moca.store.work

import java.nio.file._

import akka.actor.ActorSystem
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.serialization.SerializerService
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.Future
import scala.util.{Random, Try}

class MapDBWorkRepo(config: Config, system: ActorSystem, partition: PartitionSelector, bus: EventBus, serializers: SerializerService) extends WorkRepo with StrictLogging {

  val base = Paths.get(config.getString("directory"))
  base.toFile.mkdirs()

  private val db = DBMaker
    .appendFileDB(base.resolve("__main").toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .make()

  private val works = db.hashMap[String, Array[Byte]]("works")
  private val latestResults = db.hashMap[String, Array[Byte]]("latest-results")
  private val runs = db.hashMap[String, String]("running-works")

  private val ws = serializers.create[Work]
  private val rs = serializers.create[AllContentLinksTransfer]

  private val running = mutable.HashMap.empty[String, Run]
  runs.foreach { case (workId, runId) =>
    logger.trace(s"Reopening run $runId for work $workId")
    val run = loadFor(runId, workId)
    running.put(runId, run)
    publishTasks(run)
    logger.trace(s"Run $runId has tasks ${run.allTasks.map(_.id).mkString("[", ", ", "]")}")
  }

  override def addWork(added: Set[Work]): Future[Boolean] = transaction {
    val newWorks = added.filterNot(work => works.containsKey(work.id))
    newWorks.foreach { work =>
      works.put(work.id, ws.serialize(work))
      val run = createFor(work)
      logger.debug(s"New run for work ${work.id} ${work.seed} with task root id ${run.id}")
      running.put(run.id, run)
      runs.put(work.id, run.id)
      run.initialTasks()
      publishTasks(run)
    }
    newWorks.nonEmpty
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = transaction {
    Option(latestResults.get(workId)).map(rs.deserialize)
  }

  override def addTask(parentId: String, depth: Int, urls: Set[Url]): Future[Unit] = transaction {
    val run = forId(parentId)
    run.newTasks(urls, depth)
    publishTasks(run)
  }

  override def release(taskId: String): Future[Unit] =
    releaseAll(Set(taskId))

  override def releaseAll(taskIds: Set[String]): Future[Unit] = transaction {
    taskIds.foreach { taskId =>
      val run = forId(taskId)
      run.release(taskId)
      publishTasks(run)
    }
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = transaction {
    val r = forId(taskId)
    r.complete(taskId, transfer)

    if (r.inProgress) None
    else {
      latestResults.put(r.work.id, rs.serialize(r.transfers))
      running.remove(r.id)
      runs.remove(r.work.id)
      r.close()
      Some(r.work.id)
    }
  }

  override def close(): Unit = {
    running.values.foreach(_.stop())
    running.clear()
    works.close()
    latestResults.close()
    runs.close()
    db.close()
  }

  private def transaction[T](f: => T): Future[T] = {
    try {
      val result = f
      db.commit()
      Future.successful(result)
    } catch { case e: Exception =>
      Try(db.rollback())
      Future.failed(e)
    }
  }

  private def publishTasks(run: Run): Unit = {
    run.unpublishedTasks.foreach(task => bus.publish(EventBus.NewTasks, task))
  }

  private def createFor(work: Work): Run = {
    val runId = Run.mkId(Run.runIdSize)
    val run = new Run(runId, work, base, serializers, partition)
    run
  }
  
  private def loadFor(runId: String, workId: String): Run = {
    val work = ws.deserialize(works.get(workId))
    val run = new Run(runId, work, base, serializers, partition)
    run
  }

  private def forId(taskId: String): Run = {
    val parts = taskId.split(Run.idSeparator)
    running(parts.head)
  }

}

case class Run(id: String, work: Work, directory: Path, serializers: SerializerService, partition: PartitionSelector) extends StrictLogging {

  private val file = directory.resolve(id).toFile

  private val db = DBMaker
    .appendFileDB(file)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .make()

  private val taskPublishTimestamp = db.hashMap[String, Long]("task-timestamps")
  private val tasks = db.hashMap[String, Array[Byte]]("tasks")
  private val depths = db.hashMap[Url, Int]("depths")
  private val links = db.hashMap[Int, Array[Byte]]("links")

  private val ts = serializers.create[Task]
  private val cs = serializers.create[ContentLink]

  def initialTasks(): Unit =
    newTasks(Set(work.seed), 0)

  val unscheduledTaskTimestamp = -1

  def newTasks(urls: Set[Url], depth: Int): Unit = {
    urls.groupBy(partition.apply)
      .mapValues(group => group.filter(url => depth < depths.getOrElse(url, Int.MaxValue)))
      .filter { case (_, group) => group.nonEmpty }
      .foreach { case (part, group) =>
        val taskId = s"$id${Run.idSeparator}${Run.mkId(Run.taskIdSize)}"
        val task = Task(taskId, group, work.criteria, depth, part)
        group.foreach(url => depths.put(url, depth))
        tasks.put(taskId, ts.serialize(task))
        taskPublishTimestamp.put(taskId, unscheduledTaskTimestamp)
        logger.debug(s"New task $taskId for work ${work.id}")
      }
    db.commit()
  }

  def unpublishedTasks: Set[Task] = {
    val timestamp = Platform.currentTime
    val unpublished = taskPublishTimestamp.filter { case (_, t) => t == unscheduledTaskTimestamp }.keySet.map { taskId =>
      taskPublishTimestamp.put(taskId, timestamp)
      ts.deserialize(tasks.get(taskId))
    }
    db.commit()
    unpublished.toSet
  }

  def allTasks: Set[Task] = {
    val timestamp = Platform.currentTime
    val unpublished = tasks.map { case (taskId, task) =>
      taskPublishTimestamp.put(taskId, timestamp)
      ts.deserialize(task)
    }
    db.commit()
    unpublished.toSet
  }

  def release(taskId: String): Unit = {
    if (tasks.containsKey(taskId)) {
      taskPublishTimestamp.put(taskId, unscheduledTaskTimestamp)
      db.commit()
      ts.deserialize(tasks.get(taskId))
    }
  }

  def complete(taskId: String, transfer: ContentLinksTransfer): Unit = {
    tasks.remove(taskId)
    taskPublishTimestamp.remove(taskId)
    var size = links.size()
    transfer.contents.foreach { content =>
      depths.put(content.url, math.min(content.depth, depths.get(content.url)))
      links.put(size, cs.serialize(content))
      size += 1
    }
    db.commit()
  }

  def transfers: AllContentLinksTransfer =
    AllContentLinksTransfer(links.values().map(cs.deserialize).toList)

  def isDone: Boolean =
    tasks.isEmpty

  def inProgress: Boolean =
    !isDone

  def stop(): Unit = {
    db.close()
  }

  def close(): Unit = {
    taskPublishTimestamp.close()
    tasks.close()
    depths.close()
    links.close()
    db.close()
    file.delete()
  }

}

object Run {
  
  val idSeparator = "::"
  val runIdSize = 16
  val taskIdSize = 8

  def mkId(size: Int) = Random.alphanumeric.take(size).mkString

}