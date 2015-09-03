package com.github.lucastorri.moca.store.work

import java.io.File
import java.nio.file._

import akka.actor.{ActorSystem, Props, Status}
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.util.Timeout
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.scheduler.TaskScheduler
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.{Config, ConfigMemorySize}
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Random, Try}

class JournalMapDBWorkRepo(base: Path, increment: ConfigMemorySize, system: ActorSystem, partition: PartitionSelector, scheduler: TaskScheduler) extends WorkRepo with StrictLogging {

  def this(config: Config, system: ActorSystem, partition: PartitionSelector, scheduler: TaskScheduler) = this(
    Paths.get(config.getString("directory")),
    config.getMemorySize("allocate-increment"),
    system, partition, scheduler)

  base.toFile.mkdirs()

  private val db = DBMaker
    .appendFileDB(base.resolve("__main").toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .allocateIncrement(increment.toBytes)
    .make()

  private val works = db.hashMap[String, Array[Byte]]("works")
  private val awaitingTasks = db.hashMap[String, Array[Byte]]("awaiting-tasks")
  private val latestResults = db.hashMap[String, Array[Byte]]("latest-results")

  private val ws = new KryoSerialization[Work](system)
  private val ts = new KryoSerialization[Task](system)
  private val rs = new KryoSerialization[AllContentLinksTransfer](system)
  private val cs = new KryoSerialization[ContentLink](system)

  private val journal = system.actorOf(Props[RunJournal])


  class RunJournal extends PersistentActor {

    private val running = mutable.HashMap.empty[String, Run] //TODO make it a state
    private val subscribers = mutable.HashSet.empty[TaskSubscriber]

    def createFor(work: Work, id: String): Run = {
      val run = new Run(id, work, base.resolve(id).toFile)
      running.put(run.id, run)
      run
    }

    def forId(taskId: String): Run = {
      val parts = taskId.split("::")
      running(parts.head)
    }

    override def receiveRecover: Receive = ???

    override def receiveCommand: Receive = {

      case cmd: RunJournal.Command =>
        persist(cmd) { cmd =>
          try {
            val result = handle(cmd)
            db.commit()
            sender() ! result
          } catch { case e: Exception =>
            Try(db.rollback())
            sender() ! Status.Failure(e)
          }
        }

    }

    def handle: Function[RunJournal.Command, Any] = {

      case RunJournal.AddWork(added) =>
        val newWorks = added.filterNot(work => works.containsKey(work.id))
        newWorks.foreach { work =>
          works.put(work.id, ws.serialize(work))
          val runId = Random.alphanumeric.take(16).mkString
          persist(RunJournal.RunStarting(runId, work.id)) { _ =>
            logger.debug(s"New run for work ${work.id} ${work.seed} with task root id $runId")
            val run = createFor(work, runId)
            run.initialTasks().foreach(scheduler.add)
          }
        }
        newWorks.nonEmpty

      case RunJournal.AddTask(parentId, depth, urls) =>
        forId(parentId).newTasks(urls, depth).foreach(scheduler.add)

      case RunJournal.Release(taskIds) =>
        taskIds.map(taskId => forId(taskId).release(taskId)).foreach(scheduler.add)

      case RunJournal.MarkDone(taskId, transfer) =>
        val r = forId(taskId)
        r.complete(taskId, transfer)

        if (r.isDone) persist(RunJournal.RunFinished(taskId)) { _ =>
          latestResults.put(r.work.id, rs.serialize(r.transfers))
          db.commit()
          running.remove(r.id)
          r.close()
        }

        if (r.isDone) Some(r.work.id) else None

      case RunJournal.GetLinks(workId) =>
        Option(latestResults.get(workId)).map(rs.deserialize)

    }

    override def persistenceId: String = "journal-mapdb-work-repo"

  }

  object RunJournal {

    sealed trait Event
    case class RunStarting(runId: String, workId: String) extends Event
    case class RunFinished(runId: String) extends Event

    sealed trait Command
    case class AddWork(added: Set[Work]) extends Command
    case class AddTask(parentId: String, depth: Int, urls: Set[Url]) extends Command
    case class Release(taskIds: Set[String]) extends Command
    case class MarkDone(taskId: String, transfer: ContentLinksTransfer) extends Command
    case class GetLinks(workId: String) extends Command

  }

  implicit val timeout: Timeout = 15.seconds

  override def addWork(added: Set[Work]): Future[Boolean] =
    (journal ? RunJournal.AddWork(added)).mapTo[Boolean]

  override def links(workId: String): Future[Option[ContentLinksTransfer]] =
    (journal ? RunJournal.GetLinks(workId)).mapTo[Option[ContentLinksTransfer]]

  override def addTask(parentId: String, depth: Int, urls: Set[Url]): Future[Unit] = 
    (journal ? RunJournal.AddTask(parentId, depth, urls)).mapTo[Unit]

  override def release(taskId: String): Future[Unit] =
    (journal ? RunJournal.Release(Set(taskId))).mapTo[Unit]

  override def releaseAll(taskIds: Set[String]): Future[Unit] =
    (journal ? RunJournal.Release(taskIds)).mapTo[Unit]

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = 
    (journal ? RunJournal.MarkDone(taskId, transfer)).mapTo[Option[String]]


  def close(): Unit = {
    system.stop(journal)
  }


  case class Run(id: String, work: Work, directory: File) {

    directory.mkdirs()

    private val db = DBMaker
      .appendFileDB(directory)
      .closeOnJvmShutdown()
      .cacheLRUEnable()
      .allocateIncrement(increment.toBytes)
      .make()

    private val taskPublishTimestamp = db.hashMap[String, Long]("task-timestamps") //TODO
    private val tasks = db.hashMap[String, Array[Byte]]("tasks")
    private val depths = db.hashMap[Url, Int]("depths")
    private val links = db.hashMap[Int, Array[Byte]]("links")

//    val tasks = mutable.HashMap.empty[String, Task]
//    val depths = mutable.HashMap.empty[Url, Int]
//    val links = mutable.ListBuffer.empty[ContentLink]

    def initialTasks(): Set[Task] =
      newTasks(Set(work.seed), 0)

    private val unscheduledTaskTimestamp = -1

    def newTasks(urls: Set[Url], depth: Int): Set[Task] = {
      val nt = urls.groupBy(partition.apply)
        .mapValues(group => group.filter(url => depth < depths.getOrElse(url, Int.MaxValue))) //TODO dup work on setShallowestUrlDepth
        .filter { case (_, group) => group.nonEmpty }
        .map { case (part, group) =>
          val taskId = s"$id::${Random.alphanumeric.take(8).mkString}"
          val task = Task(taskId, group, work.criteria, depth, part)
          group.foreach(url => setShallowestUrlDepth(url, depth))
          tasks.put(taskId, ts.serialize(task))
          taskPublishTimestamp.put(taskId, unscheduledTaskTimestamp)
          logger.debug(s"New task $taskId for work ${work.id}")
          task
        }
      db.commit()
      nt.toSet
    }

    def unpublishedTasks: Set[Task] = {
      val timestamp = System.currentTimeMillis()
      val unpublished = taskPublishTimestamp.filter { case (_, t) => t == unscheduledTaskTimestamp }.keySet.map { taskId =>
        taskPublishTimestamp.put(taskId, timestamp)
        ts.deserialize(tasks.get(taskId))
      }
      db.commit()
      unpublished.toSet
    }

    def release(taskId: String): Task =
      ts.deserialize(tasks.get(taskId))

    def complete(taskId: String, transfer: ContentLinksTransfer): Unit = {
      tasks.remove(taskId)
      taskPublishTimestamp.remove(taskId)
      var size = links.size()
      transfer.contents.foreach { content =>
        setShallowestUrlDepth(content.url, content.depth)
        links.put(size, cs.serialize(content))
        size += 1
      }
      db.commit()
    }

    private def setShallowestUrlDepth(url: Url, depth: Int): Unit = {
      val existingDepthIsSmaller = Option(depths.get(url)).exists(_ < depth)
      if (!existingDepthIsSmaller) depths.put(url, depth)
    }

    def transfers: AllContentLinksTransfer =
      AllContentLinksTransfer(links.values().map(cs.deserialize).toList)

    def isDone: Boolean =
      tasks.isEmpty

    def close(): Unit = {
      db.close()
      FileUtils.deleteDirectory(directory)
    }

  }

}
