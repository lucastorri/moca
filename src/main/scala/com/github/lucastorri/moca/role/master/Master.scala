package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.util.Timeout
import com.github.lucastorri.moca.async.{retry, noop}
import com.github.lucastorri.moca.role.Messages.{Ack, WorkDone, WorkOffer, WorkRequest}
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.master.Master.Event.{GoingDown, WorkNotAccepted, WorkStarted, WorkerTerminated}
import com.github.lucastorri.moca.role.master.Master.{CleanUp, Reply}
import com.github.lucastorri.moca.store.work.WorkRepo

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(works: WorkRepo) extends PersistentActor {

  import context._
  implicit val timeout: Timeout = 10.seconds

  var state = State.initial()

  override def preStart(): Unit = {
    println("Master started")
    system.scheduler.schedule(5.minutes, 5.minutes, self, CleanUp)
  } 

  override def receiveRecover: Receive = {

    case _ =>
    //TODO need to ping workers, see if they are still alive, and what work they have
  }

  override def receiveCommand: Receive = {

    case WorkRequest =>
      val who = sender()
      works.next().foreach { work => self ! Reply(who, WorkOffer(work)) }

    case Reply(who, offer) =>
      persist(WorkStarted(who, offer.work)) { ws =>
        retry(3)(ws.who ? offer).filter(_ == Ack).onComplete {
          case Success(_) =>
            self ! ws
          case Failure(t) =>
            t.printStackTrace()
            persist(WorkNotAccepted(ws.who, ws.work))(self ! _)
        }
      }

    case Terminated(who) =>
      persist(WorkerTerminated(who))(noop)
      works.releaseAll(state.get(who))
      state = state.cancel(who)

    case CleanUp =>
      //TODO check work that has been running for too long
      saveSnapshot(state)
      //TODO delete old snapshots and events

    case wna @ WorkNotAccepted(who, work) =>
      state = state.cancel(who, work)

    case ws @ WorkStarted(who, work) =>
      println(ws)
      state = state.start(who, work)
      watch(ws.who)

    case WorkDone(workId) =>
      println(s"done $workId")
      works.done(workId)

  }

  override val persistenceId: String = s"${Master.name}-persistence"
  override def journalPluginId: String = "store.mem-journal"
  override def snapshotPluginId: String = "store.mem-snapshot"
}

object Master {

  val role = "master"
  val name = "master"

  def proxy()(implicit system: ActorSystem): ActorRef = {
    val path = s"/user/$name"
    val settings = ClusterSingletonProxySettings(system).withRole(role)
    system.actorOf(ClusterSingletonProxy.props(path, settings))
  }
  
  def join(work: WorkRepo)(implicit system: ActorSystem): ActorRef = {
    val settings = ClusterSingletonManagerSettings(system).withRole(role)
    val manager = ClusterSingletonManager.props(Props(new Master(work)), GoingDown, settings)
    system.actorOf(manager, name)
  }

  sealed trait Event
  object Event {
    case class WorkStarted(who: ActorRef, work: Work) extends Event
    case class WorkNotAccepted(who: ActorRef, work: Work)
    case class WorkerTerminated(who: ActorRef) extends Event
    case object GoingDown extends Event
  }
  
  case object CleanUp
  case class Reply(who: ActorRef, offer: WorkOffer)

}
