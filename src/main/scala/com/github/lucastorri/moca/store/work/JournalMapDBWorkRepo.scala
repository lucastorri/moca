package com.github.lucastorri.moca.store.work

import java.io.File
import java.nio.file._

import akka.actor.{ActorSystem, Props, Status}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.github.lucastorri.moca.store.work.RunJournal._
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Random, Try}

class JournalMapDBWorkRepo(config: Config, system: ActorSystem, partition: PartitionSelector, bus: EventBus) extends WorkRepo with StrictLogging {

  private val journal = system.actorOf(Props(new RunJournal(config, partition, bus)))

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

  override def republishAllTasks(): Future[Unit] =
    (journal ? RunJournal.PublishAll).mapTo[Unit]

  override def close(): Unit = {
    system.stop(journal)
  }

}

class RunJournal(config: Config, partition: PartitionSelector, bus: EventBus) extends PersistentActor with StrictLogging {

  import context._

  val base = Paths.get(config.getString("directory"))
  base.toFile.mkdirs()

  private val db = DBMaker
    .appendFileDB(base.resolve("__main").toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .make()

  val snapshotInterval = 10.minutes
  private val works = db.hashMap[String, Array[Byte]]("works")
  private val latestResults = db.hashMap[String, Array[Byte]]("latest-results")

  private val ws = new KryoSerialization[Work](system)
  private val rs = new KryoSerialization[AllContentLinksTransfer](system)

  private val running = mutable.HashMap.empty[String, Run]
  private var journalNumberOnSnapshot = 0L

  override def preStart(): Unit = {
    system.scheduler.schedule(snapshotInterval, snapshotInterval, self, Snapshot)
  }

  override def postStop(): Unit = {
    super.postStop()
    running.values.foreach(_.stop())
    db.close()
  }

  def createFor(runId: String, work: Work): Run = {
    val run = new Run(runId, work, base.resolve(runId).toFile, system, partition)
    running.put(run.id, run)
    run
  }
  
  def loadFor(runId: String, workId: String): Run = {
    val work = ws.deserialize(works.get(workId))
    val run = new Run(runId, work, base.resolve(runId).toFile, system, partition)
    running.put(run.id, run)
    run
  }

  def forId(taskId: String): Run = {
    val parts = taskId.split("::")
    running(parts.head)
  }

  override def receiveRecover: Receive = {
    
    case e: Event => e match {

      case RunStarting(runId, workId) =>
        running.put(runId, loadFor(runId, workId))

      case RunFinished(runId) =>
        running.remove(runId)
        
    }

    case SnapshotOffer(meta, State(runs)) =>
      runs.foreach(start => running.put(start.runId, loadFor(start.runId, start.workId)))

    case RecoveryCompleted =>

  } 

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

    case PublishTasks(run) =>
      run.unpublishedTasks.foreach(task => bus.publish(EventBus.NewTasks, task))

    case PublishAll =>
      running.values.foreach(run => run.allTasks.foreach(task => bus.publish(EventBus.NewTasks, task)))

    case Snapshot =>
      journalNumberOnSnapshot = lastSequenceNr
      val snapshot = running.map { case (runId, run) => RunStarting(runId, run.work.id) }.toSet
      saveSnapshot(State(snapshot))

    case SaveSnapshotSuccess(meta) =>
      deleteMessages(journalNumberOnSnapshot - 1)
      deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))
      
  }

  def handle: Function[RunJournal.Command, Any] = {

    case RunJournal.AddWork(added) =>
      val newWorks = added.filterNot(work => works.containsKey(work.id))
      newWorks.foreach { work =>
        works.put(work.id, ws.serialize(work))
        val runId = Random.alphanumeric.take(16).mkString
        persist(RunJournal.RunStarting(runId, work.id)) { _ =>
          logger.debug(s"New run for work ${work.id} ${work.seed} with task root id $runId")
          val run = createFor(runId, work)
          run.initialTasks()
          self ! PublishTasks(run)
        }
      }
      newWorks.nonEmpty

    case RunJournal.AddTask(parentId, depth, urls) =>
      val run = forId(parentId)
      run.newTasks(urls, depth)
      self ! PublishTasks(run)

    case RunJournal.Release(taskIds) =>
      taskIds.foreach { taskId =>
        val run = forId(taskId)
        run.release(taskId)
        self ! PublishTasks(run)
      }

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

  case class PublishTasks(run: Run)

  override def persistenceId: String = "journal-mapdb-work-repo"
  override def journalPluginId: String = config.getString("journal-plugin-id")
  override def snapshotPluginId: String = config.getString("snapshot-plugin-id")

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

  case object PublishAll
  case object Snapshot
  case class State(runs: Set[RunStarting])

}

case class Run(id: String, work: Work, directory: File, system: ActorSystem, partition: PartitionSelector) extends StrictLogging {

  private val db = DBMaker
    .appendFileDB(directory)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .make()

  private val taskPublishTimestamp = db.hashMap[String, Long]("task-timestamps")
  private val tasks = db.hashMap[String, Array[Byte]]("tasks")
  private val depths = db.hashMap[Url, Int]("depths")
  private val links = db.hashMap[Int, Array[Byte]]("links")

  private val ts = new KryoSerialization[Task](system)
  private val cs = new KryoSerialization[ContentLink](system)

  def initialTasks(): Set[Task] =
    newTasks(Set(work.seed), 0)

  val unscheduledTaskTimestamp = -1
  val separator = "::" //TODO use everywhere

  def newTasks(urls: Set[Url], depth: Int): Set[Task] = {
    val nt = urls.groupBy(partition.apply)
      .mapValues(group => group.filter(url => depth < depths.getOrElse(url, Int.MaxValue))) //TODO dup work on setShallowestUrlDepth
      .filter { case (_, group) => group.nonEmpty }
      .map { case (part, group) =>
      val taskId = s"$id$separator${Random.alphanumeric.take(8).mkString}"
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

  def allTasks: Set[Task] = {
    val timestamp = System.currentTimeMillis()
    val unpublished = tasks.map { case (taskId, task) =>
      taskPublishTimestamp.put(taskId, timestamp)
      ts.deserialize(task)
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

  def stop(): Unit = {
    db.close()
  }

  def close(): Unit = {
    db.close()
    directory.delete()
  }

}
