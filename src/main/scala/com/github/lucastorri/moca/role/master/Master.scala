package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.async.{noop, retry}
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.master.Master.Event
import com.github.lucastorri.moca.role.master.Master.Event.{TaskDone, TaskFailed, TaskStarted, WorkerDied}
import com.github.lucastorri.moca.store.work.WorkRepo
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(repo: WorkRepo) extends PersistentActor with StrictLogging {

  import context._
  implicit val timeout: Timeout = 10.seconds

  private var state = State.initial()
  private var journalNumberOnSnapshot = 0L
  private var firstClean = true
  private val mediator = DistributedPubSub(context.system).mediator

  override def preStart(): Unit = {
    logger.info("Master started")
    system.scheduler.schedule(Master.pingInterval, Master.pingInterval, self, CleanUp)
  }

  override def receiveRecover: Receive = {

    case e: Event => e match {
      case TaskStarted(who, taskId) =>
        state = state.start(who, taskId)
      case TaskFailed(who, taskId) =>
        state = state.cancel(who, taskId)
      case WorkerDied(who) =>
        state = state.cancel(who)
      case TaskDone(who, taskId) =>
        state = state.done(who, taskId)
    }
    
    case SnapshotOffer(meta, s: State) =>
      logger.info("Using snapshot")
      state = s
    
    case RecoveryCompleted =>
      logger.info("Recovered")
      logger.trace(s"State is $state")
      self ! CleanUp

  }

  override def receiveCommand: Receive = {

    case TaskRequest =>
      val who = sender()
      repo.available().onComplete {
        case Success(Some(task)) =>
          self ! Reply(who, TaskOffer(task))
        case Success(None) =>
        case Failure(t) =>
          logger.error("Could not get next task available", t)
      }

    case Reply(who, offer) =>
      persist(TaskStarted(who, offer.task.id)) { ws =>
        retry(3)(ws.who ? offer).acked.onComplete {
          case Success(_) =>
            self ! ws
          case Failure(t) =>
            logger.error(s"Failed to start task ${ws.taskId} for $who", t)
            self ! TaskFailed(ws.who, ws.taskId)
        }
      }

    case Terminated(who) =>
      logger.info(s"Worker down: $who")
      persist(WorkerDied(who))(noop)
      repo.releaseAll(state.get(who).map(_.taskId))
        .onFailure { case t => logger.error("Could not release worker tasks", t) }
      state = state.cancel(who)

    case CleanUp =>
      logger.trace("Clean up")
      if (!firstClean) saveSnapshot(state)
      journalNumberOnSnapshot = lastSequenceNr

      val toExtend = state.ongoingTasks.map { case (who, all) =>
        val toPing = all.filter(_.shouldPing || firstClean)
        toPing.foreach { ongoing =>
          retry(3)(who ? IsInProgress(ongoing.taskId)).acked.onFailure { case t =>
            logger.trace(s"$who is down")
            self ! TaskFailed(who, ongoing.taskId)
          }
        }
        who -> toPing
      }
      state = state.extendDeadline(toExtend)
      firstClean = false

    case SaveSnapshotSuccess(meta) =>
      if (journalNumberOnSnapshot - 1 > 0) deleteMessages(journalNumberOnSnapshot - 1)
      deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))

    case fail @ TaskFailed(who, taskId) =>
      logger.info(s"Task $taskId failed")
      persist(fail)(noop)
      state = state.cancel(who, taskId)
      repo.release(taskId)
        .onFailure { case t => logger.error(s"Could not release $taskId", t) }

    case TaskStarted(who, taskId) =>
      logger.info(s"Task $taskId started")
      state = state.start(who, taskId)
      watch(who)

    case TaskFinished(taskId, transfer) =>
      logger.info(s"Task $taskId done")
      val who = sender()
      repo.done(taskId, transfer).onComplete {
        case Success(finishedWorkId) =>
          finishedWorkId.foreach(id => logger.info(s"Finished run on work $id"))
          who ! Ack
          self ! Done(taskId, who)
        case Failure(t) =>
          logger.error(s"Could not mark $taskId done", t)
          who ! Nack
      }

    case Done(taskId, who) =>
      persist(TaskDone(who, taskId))(noop)
      state = state.done(who, taskId)

    case ConsistencyCheck =>
      //TODO check if any work that was made available is not on the current state
      sender() ! Ack

    case AddWork(seeds) =>
      logger.trace("Adding new seeds")
      val who = sender()
      repo.addWork(seeds).onComplete {
        case Success(_) =>
          who ! Ack
          announceTasksAvailable()
        case Failure(t) =>
          logger.error("Could not add seeds", t)
      }

    case AddSubTask(taskId, depth, urls) =>
      val who = sender()
      repo.addTask(taskId, depth, urls).onComplete {
        case Success(_) =>
          who ! Ack
          announceTasksAvailable()
        case Failure(t) =>
          logger.error("Could not add sub-task", t)
          who ! Nack
      }

    case GetLinks(taskId) =>
      val who = sender()
      repo.links(taskId).onComplete {
        case Success(transfer) =>
          who ! WorkLinks(taskId, transfer)
        case Failure(t) =>
          logger.error("Could not retrieve links", t)
          who ! Nack
      }

  }

  def announceTasksAvailable(): Unit =
    mediator ! DistributedPubSubMediator.Publish(TasksAvailable.topic, TasksAvailable)

  override def unhandled(message: Any): Unit = message match {
    case _: DeleteSnapshotsSuccess =>
    case _: DeleteMessagesSuccess =>
    case _ => logger.error(s"Unknown message $message")
  }
    
  override val persistenceId: String = Master.name
  override def journalPluginId: String = system.settings.config.getString("moca.master.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.snapshot-plugin-id")

  case object CleanUp
  case class Reply(who: ActorRef, offer: TaskOffer)
  case class Done(taskId: String, who: ActorRef)

}

object Master {

  val role = "master"
  val name = "master"

  val pingInterval = 5.minutes
  val consistencyCheckInterval = 30.minutes

  def proxy()(implicit system: ActorSystem): ActorRef = {
    val path = s"/user/$name"
    val settings = ClusterSingletonProxySettings(system).withRole(role)
    system.actorOf(ClusterSingletonProxy.props(path, settings))
  }
  
  def standBy(work: WorkRepo)(implicit system: ActorSystem): Unit = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role)
    val manager = ClusterSingletonManager.props(Props(new Master(work)), PoisonPill, settings)
    system.actorOf(manager, name)
  }

  sealed trait Event
  object Event {
    case class TaskStarted(who: ActorRef, taskId: String) extends Event
    case class TaskFailed(who: ActorRef, taskId: String) extends Event
    case class TaskDone(who: ActorRef, taskId: String) extends Event
    case class WorkerDied(who: ActorRef) extends Event
  }

}
