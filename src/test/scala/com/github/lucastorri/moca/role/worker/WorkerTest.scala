package com.github.lucastorri.moca.role.worker

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import akka.actor.{Actor, ActorRef, Props}
import com.github.lucastorri.moca.browser._
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.partition.ByHostPartitionSelector
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.{RoleTest, Task}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer, ContentRepo, TaskContentRepo}
import com.github.lucastorri.moca.store.control.{EmptyCriteria, FixedTransfer}
import com.github.lucastorri.moca.url.Url
import org.scalatest.{FlatSpec, MustMatchers}

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try

class WorkerTest extends FlatSpec with MustMatchers with RoleTest {

  override val port = 9897
  override val config = """
      |moca.minion.journal-plugin-id = "journal"
      |
      |journal {
      |  class = "com.github.lucastorri.moca.role.EmptyJournal"
      |  plugin-dispatcher = "akka.actor.default-dispatcher"
      |}
    """.stripMargin


  it must "perform assigned tasks" in new context {

    var first = true

    val onFirstRequest = Promise[ActorRef]()
    val onTaskFinished = Promise[(ActorRef, ContentLinksTransfer)]()
    val url = Url("http://www.example.com")
    val nextUrl = url.resolve("next")
    val TaskId = "1"

    master {
      case TaskRequest(who) if first =>
        first = false
        onFirstRequest.success(who)
        who ! TaskOffer(task(TaskId, url, PlusOneCriteria(url, nextUrl)))
        None
      case TaskRequest(who) =>
        Some(Nack)
      case TaskFinished(who, TaskId, transfer) =>
        onTaskFinished.success((who, transfer))
        Some(Ack)
    }
    
    withWorker { worker =>

      val startedBy = result(onFirstRequest)
      startedBy must be (worker)

      val (finishedBy, finishedTransfer) = result(onTaskFinished)

      finishedBy must be (startedBy)

      finishedTransfer.contents must equal (Seq(
        ContentLink(url, "", 0, ""), ContentLink(nextUrl, "", 1, "")))

      browser.urls.map(_._1) must equal (Seq(url, nextUrl))
      browser.urls.last._2 - browser.urls.head._2 must be >= requestsInterval.toMillis

    }
    
  }

  it must "abort if master doesn't ack end of task" in new context {

    var first = true

    val onAbort = Promise[Long]()
    val onNextRequest = Promise[Long]()

    val url = Url("http://www.example.com")
    val TaskId = "1"

    master {
      case TaskRequest(who) if first =>
        first = false
        who ! TaskOffer(task(TaskId, url, EmptyCriteria))
        None
      case TaskRequest(who) =>
        time(onNextRequest)
        Some(Nack)
      case TaskFinished(who, TaskId, transfer) =>
        Some(Nack)
      case AbortTask(who, TaskId) =>
        time(onAbort)
        Some(Ack)
    }

    withWorker { worker =>

      result(onNextRequest) must be > result(onAbort)

    }

  }

  it must "inform partitions" in new context {

    var first = true

    val onAddSubTask = Promise[Long]()
    val onFinished = Promise[Long]()

    val url = Url("http://www.example1.com")
    val nextUrl = Url("http://www.example2.com")
    val TaskId = "1"

    master {
      case TaskRequest(who) if first =>
        first = false
        who ! TaskOffer(task(TaskId, url, PlusOneCriteria(url, nextUrl)))
        None
      case TaskRequest(who) =>
        Some(Nack)
      case AddSubTask(TaskId, 1, urls) if urls == Set(nextUrl) =>
        time(onAddSubTask)
        Some(Ack)
      case TaskFinished(who, TaskId, _) =>
        time(onFinished)
        Some(Ack)
    }

    withWorker { worker =>

      result(onFinished) must be > result(onAddSubTask)

    }

  }

  it must "abort task if cannot inform of partitions" in new context {

    var first = true

    val onAbort = Promise[Long]()
    val onAddSubTask = Promise[Long]()
    val onNextRequest = Promise[Long]()

    val url = Url("http://www.example1.com")
    val nextUrl = Url("http://www.example2.com")
    val TaskId = "1"

    master {
      case TaskRequest(who) if first =>
        first = false
        who ! TaskOffer(task(TaskId, url, PlusOneCriteria(url, nextUrl)))
        None
      case TaskRequest(who) =>
        time(onNextRequest)
        Some(Nack)
      case AddSubTask(TaskId, 1, urls) if urls == Set(nextUrl) =>
        time(onAddSubTask)
        Some(Nack)
      case AbortTask(who, TaskId) =>
        time(onAbort)
        Some(Ack)
    }
    
    withWorker { worker =>

      result(onNextRequest) must be > result(onAbort)
      result(onAbort) must be > result(onAddSubTask)
      
    }

  }


  trait context {

    private var _master: ActorRef = _

    val repo = new FakeContentRepo
    val browser = new FakeBrowserProvider()
    val partition = new ByHostPartitionSelector
    val requestsInterval = 150.millis
    def master = _master

    def master(handler: PartialFunction[Any, Option[Any]]): Unit = {
      _master = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case msg if handler.isDefinedAt(msg) => handler.lift(msg).flatten.foreach(sender() ! _)
          case msg => fail(s"Unexpected message $msg")
        }
      }))
    }

    def withWorker(f: ActorRef => Unit): Unit = {
      val worker = system.actorOf(Props(new Worker(_master, repo, browser, partition, false)))
      f(worker)
      system.stop(worker)
      system.stop(_master)
    }

    def time(promise: Promise[Long]): Unit = {
      promise.success(Platform.currentTime)
      Thread.sleep(1)
    }

    def task(id: String, seed: Url, criteria: LinkSelectionCriteria): Task = {
      new Task(id, Set(seed), criteria, 0, partition(seed)) {
        override def intervalBetweenRequests: FiniteDuration = requestsInterval
      }
    }

  }

}

case class PlusOneCriteria(base: Url, next: Url) extends LinkSelectionCriteria {
  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = {
    if (link.url == base) Set(next)
    else Set.empty
  }
}

class FakeContentRepo extends ContentRepo {

  val saved = mutable.ListBuffer.empty[(Url, Int)]

  override def apply(task: Task): TaskContentRepo = new TaskContentRepo {
    override def save(url: Url, depth: Int, content: Try[Content]): Future[Unit] = {
      saved += (url -> depth)
      Future.successful(())
    }
  }

  override def links(task: Task): ContentLinksTransfer = {
    val links = saved.map { case (url, depth) => ContentLink(url, "", depth, "") }
    FixedTransfer(links: _*)
  }

}

class FakeBrowserProvider extends BrowserProvider {
  
  val urls = mutable.ListBuffer.empty[(Url, Long)]
  
  override def instance(): Browser = new Browser {
    override def goTo[T](url: Url)(f: (RenderedPage) => T): Future[T] = {
      urls += (url -> Platform.currentTime)
      val r = f(new RenderedPage {

        override def originalUrl: Url = url

        override def renderedContent: Content = Content(200, Map.empty, ByteBuffer.allocate(0), "")

        override def renderedHtml: String = ""

        override def renderedUrl: Url = url

        override def settings: BrowserSettings = BrowserSettings(StandardCharsets.UTF_8, 5.seconds, "")

        override def exec(javascript: String): AnyRef = sys.error("Not supported")

        override def originalContent: Content = renderedContent

      })
      Future.successful(r)
    }
  }

}