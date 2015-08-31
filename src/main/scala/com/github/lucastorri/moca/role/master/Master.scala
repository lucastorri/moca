package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence._
import akka.util.Timeout
import com.github.lucastorri.moca.async.{noop, retry}
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.master.Master.Event.{WorkDone, WorkFailed, WorkStarted, WorkerTerminated}
import com.github.lucastorri.moca.role.master.Master.{ConsistencyCheck, CleanUp, Event, Reply}
import com.github.lucastorri.moca.store.work.WorkRepo
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(works: WorkRepo) extends PersistentActor with StrictLogging {

  import context._
  implicit val timeout: Timeout = 10.seconds

  private var state = State.initial()
  private var journalNumberOnSnapshot = 0L
  private var firstClean = true

  override def preStart(): Unit = {
    logger.info("Master started")
    system.scheduler.schedule(Master.pingInterval, Master.pingInterval, self, CleanUp)
    system.scheduler.schedule(Master.pingInterval, Master.pingInterval, self, ConsistencyCheck)
  }

  override def receiveRecover: Receive = {

    case e: Event => e match {
      case WorkStarted(who, workId) =>
        state = state.start(who, workId)
      case WorkFailed(who, workId) =>
        state = state.cancel(who, workId)
      case WorkerTerminated(who) =>
        state = state.cancel(who)
      case WorkDone(who, workId) =>
        state = state.done(who, workId)
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

    case WorkRequest =>
      val who = sender()
      works.available().foreach { work => self ! Reply(who, WorkOffer(work)) }

    case Reply(who, offer) =>
      persist(WorkStarted(who, offer.work.id)) { ws =>
        retry(3)(ws.who ? offer).acked.onComplete {
          case Success(_) =>
            self ! ws
          case Failure(t) =>
            logger.error(s"Could not start work ${ws.workId} ${offer.work.seed} for $who", t)
            self ! WorkFailed(ws.who, ws.workId)
        }
      }

    case Terminated(who) =>
      logger.info(s"Worker down: $who")
      persist(WorkerTerminated(who))(noop)
      works.releaseAll(state.get(who).map(_.workId))
      state = state.cancel(who)

    case CleanUp =>
      logger.trace("Clean up")
      saveSnapshot(state)
      journalNumberOnSnapshot = lastSequenceNr

      val toExtend = state.ongoingWork.map { case (who, all) =>
        val toPing = all.filter(_.shouldPing || firstClean)
        toPing.foreach { ongoing =>
          retry(3)(who ? InProgress(ongoing.workId)).acked.onFailure { case t =>
            logger.trace(s"$who is down")
            self ! WorkFailed(who, ongoing.workId)
          }
        }
        who -> toPing
      }
      state = state.extendDeadline(toExtend)
      firstClean = false

    case ConsistencyCheck =>
      //TODO check if any work that was made available is not on the current state

    case SaveSnapshotSuccess(meta) =>
      if (journalNumberOnSnapshot - 1 > 0) deleteMessages(journalNumberOnSnapshot - 1)
      deleteSnapshots(SnapshotSelectionCriteria(meta.sequenceNr - 1, meta.timestamp, 0, 0))

    case fail @ WorkFailed(who, workId) =>
      logger.info(s"Work $workId failed")
      persist(fail)(noop)
      state = state.cancel(who, workId)
      works.release(workId)

    case WorkStarted(who, workId) =>
      logger.info(s"Work started $workId")
      state = state.start(who, workId)
      watch(who)

    case WorkFinished(workId, transfer) =>
      logger.info(s"Work done $workId")
      val who = sender()
      who ! Ack
      persist(WorkDone(who, workId))(noop)
      state = state.done(who, workId)
      works.done(workId) //TODO handle //TODO save links

    case AddSeeds(seeds) =>
      works.addAll(seeds)

  }
    
  override val persistenceId: String = s"${Master.name}-persistence"
  override def journalPluginId: String = system.settings.config.getString("moca.master.journal-plugin-id")
  override def snapshotPluginId: String = system.settings.config.getString("moca.master.snapshot-plugin-id")

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
  
  def join(work: WorkRepo)(implicit system: ActorSystem): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role)
    val manager = ClusterSingletonManager.props(Props(new Master(work)), PoisonPill, settings)
    system.actorOf(manager, name)
  }

  sealed trait Event
  object Event {
    case class WorkStarted(who: ActorRef, workId: String) extends Event
    case class WorkFailed(who: ActorRef, workId: String) extends Event
    case class WorkerTerminated(who: ActorRef) extends Event
    case class WorkDone(who: ActorRef, workId: String) extends Event
  }

  case object CleanUp
  case object ConsistencyCheck
  case class Reply(who: ActorRef, offer: WorkOffer)

}
