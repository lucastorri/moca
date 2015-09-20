package com.github.lucastorri.moca.role.worker

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.{Timeout => AskTimeout}
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.browser.BrowserProvider
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.master.{Master, MasterDown, MasterUp}
import com.github.lucastorri.moca.role.worker.Worker._
import com.github.lucastorri.moca.store.content.ContentRepo
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Worker(repo: ContentRepo, browserProvider: BrowserProvider, partition: PartitionSelector, bus: EventBus, holdOnMasterUp: Boolean) extends Actor with FSM[State, Task] with StrictLogging {

  import context._
  implicit val timeout: AskTimeout = 15.seconds
  private val requestWorkInterval = 5.minute

  private val mediator = DistributedPubSub(context.system).mediator
  private val master = Master.proxy()

  override def preStart(): Unit = {
    logger.info("Worker started")
    self ! RequestWork
    mediator ! DistributedPubSubMediator.Subscribe(TasksAvailable.topic, self)
    if (holdOnMasterUp) bus.subscribe(EventBus.MasterEvents) { e => self ! e }
  }

  override def postStop(): Unit = {
    logger.debug("Worker going down")
    if (stateName == State.Working) abortTask()
    super.postStop()
  }

  startWith(State.Idle, null)

  when(State.Idle, stateTimeout = requestWorkInterval) {

    case Event(StateTimeout | RequestWork | TasksAvailable, _) =>
      master ! TaskRequest(self)
      stay()

    case Event(TaskOffer(task), _) =>
      sender() ! Ack
      actorOf(Props(new Minion(task, browserProvider.instance(), repo(task), partition)))
      goto(State.Working) using task

    case Event(MasterUp, _) =>
      goto(State.OnHold)

    case Event(IsInProgress(taskId), task) =>
      sender() ! Nack
      stay()

  }

  when(State.Working) {

    case Event(TaskOffer(task), _) =>
      if (task == stateData) sender() ! Ack
      stay()

    case Event(Done, _) =>
      val minion = sender()
      val transfer = repo.links(stateData)
      retry(3)(master ? TaskFinished(self, stateData.id, transfer)).acked().onComplete {
        case Success(_) =>
          minion ! Continue
          self ! Finished
        case Failure(t) =>
          logger.error("Could not update master of finished task", t)
          minion ! Abort
          self ! Abort
      }
      stay()

    case Event(Partition(urls, depth), _) =>
      val minion = sender()
      retry(3)(master ? AddSubTask(stateData.id, depth, urls)).acked().onComplete {
        case Success(_) =>
          minion ! Continue
        case Failure(t) =>
          logger.error("Could not add sub-tasks and will abort task", t)
          minion ! Abort
          self ! Abort
      }
      stay()

    case Event(Abort, _) =>
      abortTask()
      goto(State.Idle) using null

    case Event(IsInProgress(taskId), task) =>
      sender() ! (if (taskId == task.id) Ack else Nack)
      stay()

    case Event(Finished, _) =>
      stopMinion()
      goto(State.Idle) using null

    case Event(MasterUp, _) =>
      abortTask()
      goto(State.OnHold) using null

  }

  when(State.OnHold) {

    case Event(MasterDown, _) =>
      goto(State.Idle)

    case Event(TaskOffer(task), _) =>
      sender() ! Nack
      stay()

    case Event(IsInProgress(taskId), task) =>
      sender() ! Nack
      stay()

  }

  onTransition {

    case State.Idle -> State.Working =>
      logger.info(s"Starting task ${nextStateData.id}")

    case State.Working -> State.Idle =>
      logger.info(s"Task ${stateData.id} done")
      self ! RequestWork

    case State.OnHold -> State.Idle =>
      self ! RequestWork

    case State.Working -> State.OnHold =>
      logger.info(s"Stopping task ${stateData.id}")

    case State.Idle -> State.OnHold =>
      logger.info("Holding worker")
      
  }

  override def unhandled(message: Any): Unit = message match {
    case _: DistributedPubSubMediator.SubscribeAck => logger.trace("Subscribed for new tasks")
    case _ => logger.error(s"Unexpected message on state $stateName: $message")
  }

  private def abortTask(): Unit = {
    retry(3)(master ? AbortTask(self, stateData.id))
    stopMinion()
  }

  private def stopMinion(): Unit = {
    children.foreach(_ ! PoisonPill)
  }

}

object Worker {

  val role = "worker"

  def start(repo: ContentRepo, browserProvider: BrowserProvider, partition: PartitionSelector, bus: EventBus, holdOnMasterUp: Boolean)(id: Int)(implicit system: ActorSystem): Unit = {
    system.actorOf(Props(new Worker(repo, browserProvider, partition, bus, holdOnMasterUp)), s"worker-$id")
  }

  sealed trait State
  object State {
    case object Working extends State
    case object Idle extends State
    case object OnHold extends State
  }

  case object RequestWork
  case object Done
  case object Abort
  case object Finished
  case class Partition(urls: Set[Url], depth: Int)
  case object Continue

}