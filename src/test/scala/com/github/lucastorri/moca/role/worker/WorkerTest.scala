package com.github.lucastorri.moca.role.worker

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import akka.actor.Actor.Receive
import akka.actor.{Actor, Props, ActorRef}
import com.github.lucastorri.moca.browser._
import com.github.lucastorri.moca.partition.ByHostPartitionSelector
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.{Task, RoleTest}
import com.github.lucastorri.moca.store.content.{ContentLink, TaskContentRepo, ContentLinksTransfer, ContentRepo}
import com.github.lucastorri.moca.store.control.{FakeTransfer, FakeCriteria}
import com.github.lucastorri.moca.url.Url
import org.scalatest.{MustMatchers, FlatSpec}
import scala.concurrent.duration._

import scala.concurrent.{Promise, Future}
import scala.collection.mutable
import scala.util.Try

class WorkerTest extends FlatSpec with MustMatchers with RoleTest {

  override val port = 9897
  override val name = "WorkerTest"
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
    val taskId = "1"

    master {
      case TaskRequest(who) if first =>
        first = false
        onFirstRequest.success(who)
        who ! TaskOffer(new Task(taskId, Set(url), FakeCriteria, 0, partition(url)) {
          override def intervalBetweenRequests: FiniteDuration = 50.millis
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
      finishedTransfer.contents must equal (Seq(ContentLink(url, "", 0, "")))

      browser.urls must equal (Seq(url))

    }
    
  }


  trait context {

    private var master: ActorRef = _

    val repo = new FakeContentRepo
    val browser = new FakeBrowserProvider()
    val partition = new ByHostPartitionSelector

    def master(handler: PartialFunction[Any, Option[Any]]): Unit = {
      master = system.actorOf(Props(new Actor {
        override def receive: Receive = {
          case msg if handler.isDefinedAt(msg) => handler.lift(msg).flatten.foreach(sender() ! _)
          case msg => fail(s"Unexpected message $msg")
        }
      }))
    }

    def withWorker(f: ActorRef => Unit): Unit = {
      val worker = system.actorOf(Props(new Worker(master, repo, browser, partition, false)))
      f(worker)
      system.stop(worker)
      system.stop(master)
    }

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
    FakeTransfer(links: _*)
  }

}

class FakeBrowserProvider extends BrowserProvider {
  
  val urls = mutable.ListBuffer.empty[Url]
  
  override def instance(): Browser = new Browser {
    override def goTo[T](url: Url)(f: (RenderedPage) => T): Future[T] = {
      urls += url
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