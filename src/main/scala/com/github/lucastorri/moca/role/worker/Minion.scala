package com.github.lucastorri.moca.role.worker

import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.github.lucastorri.moca.async.noop
import com.github.lucastorri.moca.browser.Browser
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.Minion.Event.{Fetched, Found, NotFetched}
import com.github.lucastorri.moca.role.worker.Minion.{Event, Next}
import com.github.lucastorri.moca.role.worker.Worker.Done
import com.github.lucastorri.moca.url.Url

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Minion(work: Work, browser: Browser) extends PersistentActor {

  import context._

  val intervalBetweenRequests = 10.seconds
  private val downloaded = mutable.HashSet.empty[Int]
  private val outstanding = mutable.LinkedHashSet.empty[Url]

  override def receiveRecover: Receive = {

    case e: Event => e match {

      case Found(urls @ _*) =>
        urls.foreach(addToQueue)

      case Fetched(url, Found(urls @ _*)) =>
        markFetched(url)
        urls.foreach(addToQueue)

      case NotFetched(url, _) =>
        markFetched(url)

    }

    case RecoveryCompleted =>
      val isNewJob = lastSequenceNr <= 0L
      if (isNewJob) self ! Found(work.seed)
      self ! Next

  }

  override def receiveCommand: Receive = {

    case Next =>
      if (outstanding.isEmpty) finish()
      else {
        val url = outstanding.head
        fetch(url).onComplete {
          case Success(fetched) => self ! fetched
          case Failure(t) => self ! NotFetched(url, t)
        }
      }

    case e: Event =>
      e match {
        case found @ Found(urls @ _*) =>
          urls.foreach(addToQueue)

        case fetched @ Fetched(url, Found(urls @ _*)) =>
          markFetched(url)
          urls.foreach(addToQueue)
          scheduleNext()

        case notFetched @ NotFetched(url, t) =>
          markFetched(url)
          scheduleNext()
          t.printStackTrace()
          //TODO log t

      }
      persist(e)(noop)

  }

  def fetch(url: Url): Future[Fetched] = {
    println(s"started $url")
    browser.goTo(url) { page =>
      println(s"doing $url")
      Fetched(url, Found(page.links.toSeq: _*))
    }
  }

  def addToQueue(url: Url): Unit =
    if (!downloaded.contains(url.hashCode) && !outstanding.contains(url)) {
      outstanding += url
    }

  def markFetched(url: Url): Unit = {
    outstanding.remove(url)
    downloaded += url.hashCode
  }

  def scheduleNext(): Unit = {
    system.scheduler.scheduleOnce(intervalBetweenRequests, self, Next)
  }

  def finish(): Unit = {
    parent ! Done
  }
  
  override val persistenceId: String = s"minion-$work"
  override def journalPluginId: String = "store.mem-journal"

}

object Minion {

  sealed trait Event
  object Event {
    case class Found(url: Url*) extends Event
    case class Fetched(url: Url, found: Found) extends Event
    case class NotFetched(url: Url, error: Throwable) extends Event
  }

  case object Next

}