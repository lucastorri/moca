package com.github.lucastorri.moca.role.worker

import akka.actor._
import com.github.lucastorri.moca.browser.Browser
import com.github.lucastorri.moca.role.Messages.{Ack, WorkDone, WorkOffer, WorkRequest}
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker.{Done, RequestWork, State}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._

class Worker extends Actor with FSM[State, Option[Work]] with StrictLogging {

  import context._

  val requestWorkInterval = 5.minute
  val master = Master.proxy()

  override def preStart(): Unit = {
    logger.info("worker started")
    self ! RequestWork
  }

  startWith(State.Idle, Option.empty)

  when(State.Idle, stateTimeout = requestWorkInterval) {

    case Event(StateTimeout | RequestWork, _) =>
      master ! WorkRequest
      stay()

    case Event(WorkOffer(work), _) =>
      sender() ! Ack
      goto(State.Working) using Some(work)

  }

  when(State.Working) {

    case Event(Done, _) =>
      sender() ! PoisonPill
      goto(State.Idle) using None

  }

  onTransition {

    case State.Idle -> State.Working =>
      val work = nextStateData.get
      actorOf(Props(new Minion(work, Browser.instance())))
      log.info(s"Starting work ${work.id} ${work.seed}")

    case State.Working -> State.Idle =>
      val work = stateData.get
      log.info(s"Work done ${work.id}")
      master ! WorkDone(work.id)
      self ! RequestWork

  }

}

object Worker {

  val role = "worker"

  def start(id: Int)(implicit system: ActorSystem): ActorRef = {
    system.actorOf(Props[Worker], s"worker-$id")
  }

  sealed trait State
  object State {
    case object Working extends State
    case object Idle extends State
  }

  case object RequestWork
  case object Done

}