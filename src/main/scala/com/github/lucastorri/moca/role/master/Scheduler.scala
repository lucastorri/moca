package com.github.lucastorri.moca.role.master

import akka.actor.{Props, ActorRefFactory, ActorRef, Status}
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.util.Timeout
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.master.Scheduler.Event
import com.github.lucastorri.moca.role.master.Scheduler.Event._
import com.github.lucastorri.moca.store.work.TasksSubscriber
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.pattern.ask


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

//TODO fix others: It is guaranteed that no new commands will be received by a persistent actor between a call to `persist` and the execution of its `handler`.
//TODO use snapshots, make State immutable
class SchedulerActor extends PersistentActor with StrictLogging {

  import context._

  private val partitionQueues = mutable.HashMap.empty[String, mutable.ListBuffer[Task]]
  private val lockedPartitions = mutable.HashSet.empty[String]
  private val taskPartition = mutable.HashMap.empty[String, String]
  private val scheduled = mutable.ListBuffer.empty[Task]

  private def add(task: Task): Unit = {
    if (lockedPartitions.contains(task.partition)) {
      val queue = partitionQueues.getOrElseUpdate(task.partition, mutable.ListBuffer.empty)
      queue.append(task)
    } else {
      lockedPartitions.add(task.partition)
      scheduled.append(task)
    }
  }

  private def next(): Option[Task] = {
    val next = scheduled.headOption
    next.foreach { task =>
      scheduled.remove(0)
      taskPartition.put(task.id, task.partition)
    }
    next
  }

  private def remove(taskId: String): Unit = {
    val partition = taskPartition(taskId)
    partitionQueues.get(partition) match {
      case Some(queue) =>
        scheduled.append(queue.remove(0))
        if (queue.isEmpty) partitionQueues.remove(partition)
      case None =>
        lockedPartitions.remove(partition)
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

  override def receiveRecover: Receive = {

    case e: Event =>
      handle(e)

    case RecoveryCompleted =>

  }

  override def receiveCommand: Receive = {

    case e: Event =>
      persist(e)(e => sender() ! handle(e))

  }

  def handle: Function[Event, Any] = {

    case Add(task) =>
      add(task)

    case Next =>
      next()

    case Release(taskIds @ _*) =>
      taskIds.foreach(taskId => remove(taskId))

  }

  override def persistenceId: String = "task-scheduler"
  override def journalPluginId: String = system.settings.config.getString("moca.master.scheduler.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.scheduler.snapshot-plugin-id")

}