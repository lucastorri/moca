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
    val onTaskFinished = Promise[(ActorRef, String, ContentLinksTransfer)]()
    val url = Url("http://www.example.com")
    val nextUrl = url.resolve("next")
    val taskId = "1"
    val interval = 300.millis

    master {
      case TaskRequest(who) if first =>
        first = false
        onFirstRequest.success(who)
        who ! TaskOffer(new Task(taskId, Set(url), new PlusOneCriteria(url, nextUrl), 0, partition(url)) {
          override def intervalBetweenRequests: FiniteDuration = interval
        })
        None
      case TaskRequest(who) =>
        Some(Nack)
      case TaskFinished(who, finishedTaskId, transfer) =>
        onTaskFinished.success((who, finishedTaskId, transfer))
        Some(Ack)
    }
    
    withWorker { worker =>

      val startedBy = result(onFirstRequest.future)
      startedBy must be (worker)

      val (finishedBy, finishedTaskId, finishedTransfer) = result(onTaskFinished.future)

      finishedTaskId must equal (taskId)
      finishedBy must be (startedBy)

      finishedTransfer.contents must equal (Seq(
        ContentLink(url, "", 0, ""), ContentLink(nextUrl, "", 1, "")))

      browser.urls.map(_._1) must equal (Seq(url, nextUrl))
      browser.urls.last._2 - browser.urls.head._2 must be >= interval.toMillis

    }
    
  }

  it must "abort if master doesn't ack end of task" in new context {

    var first = true

    val onAbort = Promise[(ActorRef, String, Long)]()
    val onNextRequest = Promise[Long]()
    val url = Url("http://www.example.com")
    val taskId = "1"
    val interval = 10.millis

    master {
      case TaskRequest(who) if first =>
        first = false
        who ! TaskOffer(new Task(taskId, Set(url), EmptyCriteria, 0, partition(url)) {
          override def intervalBetweenRequests: FiniteDuration = interval
        })
        None
      case TaskRequest(who) =>
        Thread.sleep(1)
        onNextRequest.success(Platform.currentTime)
        Some(Nack)
      case TaskFinished(who, finishedTaskId, transfer) =>
        Some(Nack)
      case AbortTask(who, abortedTaskId) =>
        onAbort.success((who, abortedTaskId, Platform.currentTime))
        Some(Ack)
    }

    withWorker { worker =>

      val (who, abortedTaskId, abortTimestamp) = result(onAbort.future)
      who must be (worker)
      abortedTaskId must equal (taskId)

      result(onNextRequest.future) must be > abortTimestamp

    }

  }


  trait context {

    private var _master: ActorRef = _

    val repo = new FakeContentRepo
    val browser = new FakeBrowserProvider()
    val partition = new ByHostPartitionSelector
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

  }

}

class PlusOneCriteria(base: Url, next: Url) extends LinkSelectionCriteria {
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