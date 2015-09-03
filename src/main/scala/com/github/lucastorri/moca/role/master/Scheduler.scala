package com.github.lucastorri.moca.role.master

import akka.actor.{ActorRefFactory, Props, Status}
import akka.pattern.ask
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.util.Timeout
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.master.Scheduler.Event
import com.github.lucastorri.moca.role.master.Scheduler.Event._
import com.github.lucastorri.moca.role.master.SchedulerActor.State
import com.github.lucastorri.moca.store.work.TasksSubscriber
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

case class Scheduler(actorFactory: ActorRefFactory) {

  private implicit val timeout: Timeout = 15.seconds
  private val scheduler = actorFactory.actorOf(Props[SchedulerActor])

  def add(task: Task): Future[Unit] =
    (scheduler ? Add(task)).mapTo[Unit]

  def next(): Future[Option[Task]] =
    (scheduler ? Next).mapTo[Option[Task]]

  def release(taskIds: String*): Future[Unit] =
    (scheduler ? Release(taskIds: _*)).mapTo[Unit]

  def asSubscriber: TasksSubscriber = new TasksSubscriber {
    override def newTask(task: Task): Unit = add(task)
  }

}

object Scheduler {

  sealed trait Event
  object Event {
    case class Add(task: Task) extends Event
    case object Next extends Event
    case class Release(taskIds: String*) extends Event
  }

}

//TODO use snapshots, make State immutable
class SchedulerActor extends PersistentActor with StrictLogging {

  import context._

  private var state = State.initial()

  private val partitionQueues = mutable.HashMap.empty[String, mutable.ListBuffer[Task]]
  private val lockedPartitions = mutable.HashSet.empty[String]
  private val taskPartition = mutable.HashMap.empty[String, String]
  private val scheduled = mutable.ListBuffer.empty[Task]

  override def receiveRecover: Receive = {

    case e: Event =>
      handle(e)

    case RecoveryCompleted =>
      state = State.removeRepeatedTasks(state)
  }

  override def receiveCommand: Receive = {

    case e: Event =>
      persist(e)(e => sender() ! handle(e))

  }

  def handle: Function[Event, Any] = {

    case Add(task) =>
      if (lockedPartitions.contains(task.partition)) {
        val queue = partitionQueues.getOrElseUpdate(task.partition, mutable.ListBuffer.empty)
        queue.append(task)
      } else {
        lockedPartitions.add(task.partition)
        scheduled.append(task)
      }

    case Next =>
      val next = scheduled.headOption
      next.foreach { task =>
        scheduled.remove(0)
        taskPartition.put(task.id, task.partition)
      }
      next

    case Release(taskIds @ _*) =>
      taskIds.foreach { taskId =>
        val partition = taskPartition(taskId)
        partitionQueues.get(partition) match {
          case Some(queue) =>
            scheduled.append(queue.remove(0))
            if (queue.isEmpty) partitionQueues.remove(partition)
          case None =>
            lockedPartitions.remove(partition)
        }
        taskPartition.remove(taskId)
      }

  }

  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    logger.error("Persist failure", cause)
    sender() ! Status.Failure(cause)
  }

  override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
    logger.error("Persist rejected", cause)
    sender() ! Status.Failure(cause)
  }

  override def persistenceId: String = "task-scheduler"
  override def journalPluginId: String = system.settings.config.getString("moca.master.scheduler.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.scheduler.snapshot-plugin-id")

}

object SchedulerActor {


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
            // scheduled.append(queue.remove(0))
            scheduledCopy :+= queue.head
            val queueCopy = queue.tail

            // if (queue.isEmpty) partitionQueues.remove(partition)
            queuesCopy =
              if (queueCopy.isEmpty) queuesCopy - partition
              else queuesCopy + (partition -> queueCopy)
          case None =>
            // lockedPartitions.remove(partition)
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
      val allTasks = (state.queues.values.flatten ++ state.scheduled).groupBy(identity).mapValues(_.size)

      val repeated = allTasks.filter { case (task, appearances) => appearances > 1 }

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