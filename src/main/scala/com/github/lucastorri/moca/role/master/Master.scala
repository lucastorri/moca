package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence.{RecoveryCompleted, SnapshotOffer, PersistentActor, SaveSnapshotSuccess}
import akka.util.Timeout
import com.github.lucastorri.moca.async.{noop, retry}
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.master.Master.Event.{WorkFailed, WorkStarted, WorkerTerminated}
import com.github.lucastorri.moca.role.master.Master.{Event, CleanUp, Reply}
import com.github.lucastorri.moca.store.work.WorkRepo
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(works: WorkRepo) extends PersistentActor with StrictLogging {

  import context._
  implicit val timeout: Timeout = 10.seconds

  var state = State.initial()

  override def preStart(): Unit = {
    logger.info("Master started")
    system.scheduler.schedule(Master.pingInterval, Master.pingInterval, self, CleanUp)
  } 

  override def receiveRecover: Receive = {
    case e: Event => e match {

      case WorkStarted(who, work) =>
        state = state.start(who, work)

      case WorkFailed(who, work) =>
        state = state.cancel(who, work)

      case WorkerTerminated(who) =>
        state = state.cancel(who)
      
    }
    
    case SnapshotOffer(meta, s: State) =>
      logger.info("Using snapshot")
      state = s
    
    case RecoveryCompleted =>
      logger.info("Recovered")
      self ! CleanUp

  }

  override def receiveCommand: Receive = {

    case WorkRequest =>
      val who = sender()
      works.available().foreach { work => self ! Reply(who, WorkOffer(work)) }

    case Reply(who, offer) =>
      persist(WorkStarted(who, offer.work)) { ws =>
        retry(3)(ws.who ? offer).acked.onComplete {
          case Success(_) =>
            self ! ws
          case Failure(t) =>
            logger.error(s"Could not start work ${ws.work.id} ${ws.work.seed} for $who", t)
            self ! WorkFailed(ws.who, ws.work)
        }
      }

    case Terminated(who) =>
      logger.info(s"Worker down: $who")
      persist(WorkerTerminated(who))(noop)
      works.releaseAll(state.get(who).map(_.work))
      state = state.cancel(who)

    case CleanUp =>
      logger.trace("Clean up")
      saveSnapshot(state)

      val toExtend = state.ongoingWork.map { case (who, all) =>
        val toPing = all.filter(_.shouldPing)
        toPing.foreach { ongoing =>
          retry(3)(who ? InProgress(ongoing.work.id)).acked
            .onFailure { case f => self ! WorkFailed(who, ongoing.work) }
        }
        who -> toPing
      }
      state = state.extendDeadline(toExtend)

      //TODO check if any work that was made available is not on the current state

    case SaveSnapshotSuccess(meta) =>
      //TODO delete old snapshots and events

    case fail @ WorkFailed(who, work) =>
      persist(fail)(noop)
      state = state.cancel(who, work)
      works.release(work.id)

    case WorkStarted(who, work) =>
      logger.info(s"Work started ${work.id} ${work.seed}")
      state = state.start(who, work)
      watch(who)

    case done @ WorkDone(workId, links) =>
      logger.info(s"Work done $workId")
      persist(done)(noop)
      sender() ! Ack

      //TODO save links
      works.done(workId)

  }
    

  override val persistenceId: String = s"${Master.name}-persistence"
  override def journalPluginId: String = "store.mem-journal"
  override def snapshotPluginId: String = "store.mem-snapshot"
}

object Master {

  val role = "master"
  val name = "master"

  val pingInterval = 5.minutes

  def proxy()(implicit system: ActorSystem): ActorRef = {
    val path = s"/user/$name"
    val settings = ClusterSingletonProxySettings(system).withRole(role)
    system.actorOf(ClusterSingletonProxy.props(path, settings))
  }
  
  def join(work: WorkRepo)(implicit system: ActorSystem): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role)
    val manager = ClusterSingletonManager.props(Props(new Master(work)), PoisonPill, settings)
    system.actorOf(manager, name)
  }

  sealed trait Event
  object Event {
    case class WorkStarted(who: ActorRef, work: Work) extends Event
    case class WorkFailed(who: ActorRef, work: Work)
    case class WorkerTerminated(who: ActorRef) extends Event

  }

  case object CleanUp
  case class Reply(who: ActorRef, offer: WorkOffer)

}
