package com.github.lucastorri.moca.scheduler

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.scheduler.PartitionBasedScheduler.State
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._

class PartitionBasedScheduler(system: ActorSystem) extends TaskScheduler with StrictLogging {

  implicit val timeout: Timeout = 10.seconds

  private val scheduler = system.actorOf(Props[Persist])
  private var state = State.initial()

  def add(task: Task): Unit = {
    scheduler ! Add(task)
  }

  def next(): Future[Option[Task]] = {
    (scheduler ? Get).mapTo[Option[Task]]
  }

  def release(taskIds: String*): Unit = {
    scheduler ! Release(taskIds)
  }

  class Persist extends PersistentActor {

    private var journalNumberOnSnapshot = 0L

    override def receiveRecover: Receive = {

      case Add(task) =>
        if (state.scheduled.last != task && state.queues(task.partition).last != task) {
          state = state.add(task)
        }
        //TODO check if task is not scheduled already
        //it should be last one on the partition or on the scheduled seq

      case Got(taskId) =>
        if (state.nextTask.exists(_.id == taskId)) {
          state = state.nextState
        }

      case SnapshotOffer(meta, s: State) =>
        state = s

      case Release(taskIds) =>
        state = state.release(taskIds)

      case RecoveryCompleted =>

    }

    override def receiveCommand: Receive = {

      case add @ Add(task) =>
        persist(add) { _ =>
          state = state.add(task)
        }

      case Get =>
        state.nextTask.foreach { task =>
          persist(Got(task.id)) { _ =>
            state = state.nextState
            sender() ! task
          }
        }

      case release @ Release(taskIds) =>
        persist(release) { _ =>
          state = state.release(taskIds)
        }

      case Snapshot =>
        journalNumberOnSnapshot = lastSequenceNr
        saveSnapshot(state)

      case SaveSnapshotSuccess(meta) =>
        deleteMessages(journalNumberOnSnapshot - 1)
        deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))

    }

    override def unhandled(message: Any): Unit = message match {
      case _: DeleteSnapshotsSuccess =>
      case _: DeleteMessagesSuccess =>
      case _ => logger.error(s"Unknown message $message")
    }

    override def persistenceId: String = ???
  }

  case class Add(task: Task)
  case object Get
  case class Got(taskId: String)
  case class Release(taskIds: Seq[String])
  case object Snapshot


  // persist next just after got it, together with the task that was taken out, so can check if should actually reomve it
  // preventing issues with more-than-once replays

}

object PartitionBasedScheduler {

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