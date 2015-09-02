package com.github.lucastorri.moca.role.worker

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.{Timeout => AskTimeout}
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.browser.BrowserProvider
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker._
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentRepo
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Worker(repo: ContentRepo, browserProvider: BrowserProvider, partition: PartitionSelector) extends Actor with FSM[State, Task] with StrictLogging {

  import context._
  implicit val timeout: AskTimeout = 10.seconds

  private val mediator = DistributedPubSub(context.system).mediator
  private val requestWorkInterval = 5.minute
  private val master = Master.proxy()

  override def preStart(): Unit = {
    logger.info("Worker started")
    self ! RequestWork
    mediator ! DistributedPubSubMediator.Subscribe(TasksAvailable.topic, self)
  }

  startWith(State.Idle, null)

  when(State.Idle, stateTimeout = requestWorkInterval) {

    case Event(StateTimeout | RequestWork | TasksAvailable, _) =>
      master ! TaskRequest
      stay()

    case Event(TaskOffer(task), _) =>
      sender() ! Ack
      actorOf(Props(new Minion(task, browserProvider.instance(), repo(task), partition)))
      goto(State.Working) using task

  }

  when(State.Working) {

    case Event(TaskOffer(task), _) =>
      if (task == stateData) sender() ! Ack
      stay()

    case Event(Done, _) =>
      sender() ! PoisonPill
      val transfer = repo.links(stateData)
      retry(3)(master ? TaskFinished(stateData.id, transfer)).acked().onComplete {
        case Success(_) =>
          self ! Finished
        case Failure(t) =>
          logger.error("Could not update master of finished task", t)
          self ! Finished
      }
      stay()

    case Event(Partition(urls), _) =>
      val taskAdd = Future.sequence {
        urls.groupBy(_.depth).map { case (depth, links) =>
          retry(3)(master ? AddSubTask(stateData.id, depth, links.map(_.url)))
        }
      }
      taskAdd.onFailure { case t => logger.error("Could not add sub-tasks", t) } //TODO should also fail worker?
      stay()

    case Event(Finished, _) =>
      goto(State.Idle) using null

  }

  onTransition {

    case State.Idle -> State.Working =>
      logger.info(s"Starting task ${nextStateData.id}")

    case State.Working -> State.Idle =>
      logger.info(s"Task ${stateData.id} done")
      self ! RequestWork

  }

  override def unhandled(message: Any): Unit = message match {
    case _: DistributedPubSubMediator.SubscribeAck => logger.trace("Subscribed for new tasks")
    case _ => logger.error(s"Unknown message $message")
  }

}

object Worker {

  val role = "worker"

  def start(repo: ContentRepo, browserProvider: BrowserProvider, partition: PartitionSelector)(id: Int)(implicit system: ActorSystem): Unit = {
    system.actorOf(Props(new Worker(repo, browserProvider, partition)), s"worker-$id")
  }

  sealed trait State
  object State {
    case object Working extends State
    case object Finishing extends State
    case object Idle extends State
  }

  case object RequestWork
  case object Done
  case object Finished
  case class Partition(urls: Set[Link])

}