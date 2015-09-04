package com.github.lucastorri.moca.store.scheduler

import akka.actor.{ActorSystem, Props, Status}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.store.scheduler.PartitionBasedScheduler._
import com.github.lucastorri.moca.store.scheduler.SchedulerActor.{State, TakeSnapshot}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._

class PartitionBasedScheduler(system: ActorSystem) extends TaskScheduler {

  private implicit val timeout: Timeout = 15.seconds
  private val scheduler = system.actorOf(Props[SchedulerActor])

  def add(task: Task): Future[Unit] =
    (scheduler ? Add(task)).mapTo[Unit]

  def next(): Future[Option[Task]] =
    (scheduler ? Next).mapTo[Option[Task]]

  def release(taskIds: String*): Future[Unit] =
    (scheduler ? Release(taskIds: _*)).mapTo[Unit]

  def close(): Unit = {
    system.stop(scheduler)
  } 
  
}

object PartitionBasedScheduler {

  sealed trait Event
  case class Add(task: Task) extends Event
  case object Next extends Event
  case class Release(taskIds: String*) extends Event

}

class SchedulerActor extends PersistentActor with StrictLogging {

  import context._

  private val snapshotInterval = 10.minutes

  private var state = State.initial()
  private var journalNumberOnSnapshot = 0L

  override def preStart(): Unit = {
    logger.info("Scheduler starting")
    system.scheduler.schedule(snapshotInterval, snapshotInterval, self, TakeSnapshot)
  }

  override def receiveRecover: Receive = {
    case e: Event =>
      handle(e)
    case RecoveryCompleted =>
      state = State.removeRepeatedTasks(state)
  }

  override def receiveCommand: Receive = {
    case e: Event =>
      persist(e)(e => sender() ! handle(e))
    case TakeSnapshot =>
      journalNumberOnSnapshot = lastSequenceNr
      saveSnapshot(state)
    case SaveSnapshotSuccess(meta) =>
      deleteMessages(journalNumberOnSnapshot - 1)
      deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))
  }

  def handle: Function[Event, Any] = {
    case Add(task) =>
      state = state.add(task)
    case Next =>
      val next = state.nextTask
      state = state.nextState
      next
    case Release(taskIds @ _*) =>
      state = state.release(taskIds)
  }

  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    logger.error("Persist failure", cause)
    sender() ! Status.Failure(cause)
  }

  override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
    logger.error("Persist rejected", cause)
    sender() ! Status.Failure(cause)
  }

  override def unhandled(message: Any): Unit = message match {
    case _: DeleteSnapshotsSuccess =>
    case _: DeleteMessagesSuccess =>
    case _ => logger.error(s"Unknown message $message")
  }

  override def persistenceId: String = "task-scheduler"
  override def journalPluginId: String = system.settings.config.getString("moca.master.scheduler.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.scheduler.snapshot-plugin-id")

}

object SchedulerActor {

  case object TakeSnapshot

  case class State(queues: Map[String, Seq[Task]], locked: Set[String], partitions: Map[String, String], scheduled: Seq[Task]) {

    def add(task: Task): State = {
      if (locked.contains(task.partition)) {
        val queue = queues.getOrElse(task.partition, Seq.empty) :+ task
        copy(queues = queues + (task.partition -> queue))
      } else {
        copy(locked = locked + task.partition, scheduled = scheduled :+ task)
      }
    }

    def nextState: State = {
      scheduled.headOption match {
        case Some(task) =>
          copy(scheduled = scheduled.tail, partitions = partitions + (task.id -> task.partition))
        case None =>
          this
      }
    }

    def nextTask: Option[Task] = {
      scheduled.headOption
    }

    def release(taskIds: Seq[String]): State = {
      var queuesCopy = queues
      var lockedCopy = locked
      var partitionsCopy = partitions
      var scheduledCopy = scheduled

      taskIds.foreach { taskId =>
        val partition = partitions(taskId)
        queues.get(partition) match {
          case Some(queue) =>
            scheduledCopy :+= queue.head
            val queueCopy = queue.tail

            queuesCopy =
              if (queueCopy.isEmpty) queuesCopy - partition
              else queuesCopy + (partition -> queueCopy)
          case None =>
            lockedCopy -= partition
        }

        partitionsCopy = partitionsCopy - taskId
      }

      copy(queues = queuesCopy, locked = lockedCopy, partitions = partitionsCopy, scheduled = scheduledCopy)
    }

  }

  object State {
    
    def initial(): State = 
      State(Map.empty, Set.empty, Map.empty, Seq.empty)

    def removeRepeatedTasks(state: State): State = {
      val repeated = (state.queues.values.flatten ++ state.scheduled)
        .groupBy(identity)
        .mapValues(_.size)
        .filter { case (task, appearances) => appearances > 1 }

      var queuesCopy = state.queues
      repeated.foreach { case (task, appearances) =>

        var queueCopy = queuesCopy(task.partition)
        val appearancesInQueue = queueCopy.count(_ == task)
        val taskIsScheduled = appearancesInQueue != appearances

        var toRemove =
          if (taskIsScheduled) appearancesInQueue
          else appearancesInQueue - 1

        def needsToRemove(): Boolean = {
          toRemove -= 1
          toRemove >= 0
        }

        queueCopy = queueCopy.reverse.filterNot(_ == task && needsToRemove()).reverse

        queuesCopy =
          if (queueCopy.isEmpty) queuesCopy - task.partition
          else queuesCopy + (task.partition -> queueCopy)
      }
      
      state.copy(queues = queuesCopy)
    }

  }

}