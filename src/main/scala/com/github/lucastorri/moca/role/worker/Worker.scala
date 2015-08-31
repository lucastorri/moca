package com.github.lucastorri.moca.role.worker

import akka.actor._
import akka.pattern.ask
import akka.util.{Timeout => AskTimeout}
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.browser.Browser
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker._
import com.github.lucastorri.moca.store.content.{ContentRepo, InMemContentRepo}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Worker extends Actor with FSM[State, Option[Work]] with StrictLogging {

  import context._
  implicit val timeout: AskTimeout = 10.seconds
  
  val requestWorkInterval = 5.minute
  val master = Master.proxy()
  val repo: ContentRepo = new InMemContentRepo
  var currentWork: Work = _

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
      currentWork = work
      actorOf(Props(new Minion(work, Browser.instance(), repo(work))))
      goto(State.Working) using Some(work)

  }

  when(State.Working) {

    case Event(WorkOffer(work), _) =>
      if (work == currentWork) sender() ! Ack
      stay()

    case Event(Done(work), _) =>
      sender() ! PoisonPill
      val transfer = repo.links(work)
      retry(3)(master ? WorkFinished(work.id, transfer)).acked().onComplete {
        case Success(_) =>
          self ! Finished
        case Failure(t) =>
          logger.error("Could not update master of finished work", t)
          self ! Finished
      }
      stay()

    case Event(Finished, _) =>
      goto(State.Idle) using None

  }
  
  onTransition {

    case State.Idle -> State.Working =>
      logger.info(s"Starting work $nextStateData")

    case State.Working -> State.Idle =>
      logger.info(s"Work done $stateData")
      self ! RequestWork

  }

}

object Worker {

  val role = "worker"

  def start(id: Int)(implicit system: ActorSystem): Unit = {
    system.actorOf(Props[Worker], s"worker-$id")
  }

  sealed trait State
  object State {
    case object Working extends State
    case object Finishing extends State
    case object Idle extends State
  }

  case object RequestWork
  case class Done(work: Work)
  case object Finished

}