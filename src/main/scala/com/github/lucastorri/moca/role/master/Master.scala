package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.event.EventBus.MasterEvents
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.master.Master._
import com.github.lucastorri.moca.role.master.scheduler.PartitionScheduler
import com.github.lucastorri.moca.role.master.tasks.TaskHandler
import com.github.lucastorri.moca.wip.RunControl
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(repo: RunControl, bus: EventBus) extends PersistentActor with StrictLogging {

  import context._
  implicit val timeout: Timeout = 10.seconds

  private val mediator = DistributedPubSub(context.system).mediator

  private var ongoing = TaskHandler.initial()
  private var scheduler = PartitionScheduler.initial()
  private var journalNumberOnSnapshot = 0L
  private var firstClean = true

  override def preStart(): Unit = {
    logger.info("Master started")
    bus.subscribe(EventBus.NewTasks) { task => self ! NewTask(task) }
    system.scheduler.schedule(Master.pingInterval, Master.pingInterval, self, CleanUp)
    bus.publish(MasterEvents, MasterUp)
  }

  override def postStop(): Unit = {
    logger.info("Master going down")
    bus.publish(MasterEvents, MasterDown)
    repo.close()
    super.postStop()
  }

  override def receiveRecover: Receive = {
    
    case SnapshotOffer(meta, State(o, s)) =>
      logger.info("Using snapshot")
      ongoing = o
      scheduler = s

    case e: Event => e match {

      case NewTask(task) =>
        scheduler = scheduler.add(task)

      case TaskStarted(who, taskId) =>
        ongoing = ongoing.start(who, taskId)
        scheduler.next.foreach { case (task, next) =>
          if (task.id == taskId) {
            scheduler = next
          }
        }

      case TaskFailed(who, taskId) =>
        ongoing = ongoing.cancel(who, taskId)
        scheduler = scheduler.release(Set(taskId))
        
      case WorkerDied(who) =>
        val taskIds = ongoing.ongoingTasks(who).map(_.taskId)
        ongoing = ongoing.cancel(who)
        scheduler = scheduler.release(taskIds)

      case TaskDone(who, taskId) =>
        ongoing = ongoing.done(who, taskId)
        scheduler = scheduler.release(Set(taskId))

    }
    
    case RecoveryCompleted =>
      logger.info("Recovered")
      logger.trace(s"State is $ongoing + $scheduler")
      self ! CleanUp

  }

  override def receiveCommand: Receive = {

    case nt @ NewTask(task) =>
      logger.trace(s"New task ${task.id}")
      persist(nt) { _ =>
        scheduler = scheduler.add(task)
        mediator ! DistributedPubSubMediator.Publish(TasksAvailable.topic, TasksAvailable)
      }

    case TaskRequest(who) =>
      val messenger = sender()
      scheduler.next match {
        case Some((task, next)) =>
          persist(TaskStarted(who, task.id)) { _ =>
            ongoing = ongoing.start(who, task.id)
            scheduler = next
            watch(who)
            retry(3)(who ? TaskOffer(task)).acked.onFailure { case t =>
              logger.error(s"Failed to send task ${task.id} to worker", t)
              self ! TaskFailed(who, task.id)
            }
          }
        case None =>
          nack(messenger)
      }

    case Terminated(who) =>
      logger.info(s"Worker down: $who")
      persist(WorkerDied(who)) { _ =>
        val taskIds = ongoing.ongoingTasks(who).map(_.taskId)
        taskIds.foreach { taskId =>
          repo.abort(taskId).onFailure { case t =>  //TODO let control receive a set
            logger.error("Could not release worker tasks", t)
          }
        }
        ongoing = ongoing.cancel(who)
        scheduler = scheduler.release(taskIds)
      }

    case fail @ TaskFailed(who, taskId) =>
      logger.info(s"Task $taskId failed")
      persist(fail) { _ =>
        ongoing = ongoing.cancel(who, taskId)
        scheduler = scheduler.release(Set(taskId))
        repo.abort(taskId).onFailure { case t =>
          logger.error(s"Could not release $taskId", t)
        }
      }

    case AbortTask(who, taskId) =>
      val messenger = sender()
      self ! TaskFailed(who, taskId)
      ack(messenger)

    case TaskFinished(who, taskId, transfer) =>
      logger.info(s"Task $taskId done")
      val messenger = sender()
      repo.done(taskId, transfer).onComplete {
        case Success(finishedWorkId) =>
          finishedWorkId.foreach(id => logger.info(s"Finished run on work $id"))
          ack(messenger)
          self ! TaskDone(who, taskId)
        case Failure(t) =>
          logger.error(s"Could not mark $taskId done", t)
          nack(messenger)
      }

    case done @ TaskDone(who, taskId) =>
      persist(done) { _ =>
        ongoing = ongoing.done(who, taskId)
        scheduler = scheduler.release(Set(taskId))
      }

    case CleanUp =>
      logger.trace("Clean up")
      journalNumberOnSnapshot = lastSequenceNr
      if (!firstClean) saveSnapshot(State(ongoing, scheduler))

      val toExtend = ongoing.ongoingTasks().map { case (who, all) =>
        val toPing = all.filter(_.shouldPing || firstClean).map(_.taskId)
        toPing.foreach { taskId =>
          retry(3)(who ? IsInProgress(taskId)).acked.onFailure { case t =>
            logger.trace(s"$who is down")
            self ! TaskFailed(who, taskId)
          }
        }
        who -> toPing
      }
      ongoing = ongoing.extendDeadline(toExtend)
      firstClean = false
      logger.trace(s"Final state is $ongoing + $scheduler")

    case SaveSnapshotSuccess(meta) =>
      deleteMessages(journalNumberOnSnapshot - 1)
      deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))

    case ConsistencyCheck =>
      //TODO check for discrepancies between repo and state
      val messenger = sender()
      ack(messenger)

    case AddWork(seeds) =>
      logger.trace("Adding new seeds")
      val messenger = sender()
      repo.add(seeds).onComplete {
        case Success(_) =>
          ack(messenger)
        case Failure(t) =>
          logger.error("Could not add seeds", t)
          nack(messenger)
      }

    case AddSubTask(taskId, depth, urls) =>
      val messenger = sender()
      repo.subTasks(taskId, depth, urls).onComplete {
        case Success(_) =>
          ack(messenger)
        case Failure(t) =>
          logger.error("Could not add sub-task", t)
          nack(messenger)
      }

    case GetLinks(taskId) =>
      val messenger = sender()
      repo.links(taskId).onComplete {
        case Success(transfer) =>
          messenger ! WorkLinks(taskId, transfer)
        case Failure(t) =>
          logger.error("Could not retrieve links", t)
          nack(messenger)
      }

  }

  private def ack(who: ActorRef): Unit = who ! Ack
  private def nack(who: ActorRef): Unit = who ! Nack

  override def unhandled(message: Any): Unit = message match {
    case _: DeleteSnapshotsSuccess =>
    case _: DeleteMessagesSuccess =>
    case _ => logger.error(s"Unknown message $message")
  }
    
  override val persistenceId: String = Master.name
  override def journalPluginId: String = system.settings.config.getString("moca.master.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.snapshot-plugin-id")

  case object CleanUp
  
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
  
  def standBy(repo: => RunControl, bus: EventBus)(implicit system: ActorSystem): Unit = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role)
    val manager = ClusterSingletonManager.props(Props(new Master(repo, bus)), PoisonPill, settings)
    system.actorOf(manager, name)
  }

  sealed trait Event
  case class NewTask(task: Task) extends Event
  case class TaskStarted(who: ActorRef, taskId: String) extends Event
  case class TaskFailed(who: ActorRef, taskId: String) extends Event
  case class TaskDone(who: ActorRef, taskId: String) extends Event
  case class WorkerDied(who: ActorRef) extends Event

  case class State(ongoing: TaskHandler, scheduler: PartitionScheduler)

}
