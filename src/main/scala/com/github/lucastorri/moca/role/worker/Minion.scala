package com.github.lucastorri.moca.role.worker

import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.github.lucastorri.moca.async.noop
import com.github.lucastorri.moca.browser.{Browser, Content}
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.Minion.Event.{Fetched, Found, NotFetched}
import com.github.lucastorri.moca.role.worker.Minion.{Event, Next}
import com.github.lucastorri.moca.role.worker.Worker.Done
import com.github.lucastorri.moca.store.content.WorkRepo
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Minion(work: Work, browser: Browser, repo: WorkRepo) extends PersistentActor with StrictLogging {

  import context._

  private val downloaded = mutable.HashSet.empty[Int]
  private val outstanding = mutable.LinkedHashSet.empty[OutLink]

  override def receiveRecover: Receive = {

    case e: Event => e match {

      case found: Found =>
        addToQueue(found)

      case Fetched(url, found) =>
        markFetched(url)
        addToQueue(found)

      case NotFetched(url, _) =>
        markFetched(url)

    }

    case RecoveryCompleted =>
      val isNewJob = lastSequenceNr <= 0L
      if (isNewJob) self ! Found(0, Set(work.seed))
      self ! Next

  }

  override def receiveCommand: Receive = {

    case Next =>
      if (outstanding.isEmpty) finish()
      else {
        val o = outstanding.head
        val result =
          for {
            (fetched, content) <- fetch(o)
            _ <- repo.save(o.url, content)
          } yield fetched
        result.onComplete {
          case Success(fetched) =>
            self ! fetched
          case Failure(t) =>
            self ! NotFetched(o, t)
        }
      }

    case e: Event =>
      e match {
        case found: Found =>
          addToQueue(found)

        case fetched @ Fetched(url, found) =>
          logger.trace(s"Fetched $url")
          markFetched(url)
          addToQueue(found)
          scheduleNext()

        case notFetched @ NotFetched(url, t) =>
          logger.error(s"Could not fetch $url", t)
          markFetched(url)
          scheduleNext()


      }
      persist(e)(noop)

  }

  def fetch(link: OutLink): Future[(Fetched, Content)] = {
    logger.trace(s"Requesting ${link.url}")
    browser.goTo(link.url) { page =>
      logger.trace(s"Processing ${link.url}")
      Fetched(link, Found(link.depth + 1, work.select(link, page))) -> page.content
    }
  }

  def addToQueue(found: Found): Unit =
    OutLink.all(found).foreach { o =>
      if (!downloaded.contains(o.url.hashCode) && !outstanding.contains(o)) {
        outstanding += o
      }
    }

  def markFetched(link: OutLink): Unit = {
    outstanding.remove(link)
    downloaded += link.url.hashCode
  }

  def scheduleNext(): Unit = {
    system.scheduler.scheduleOnce(work.intervalBetweenRequests, self, Next)
  }

  def finish(): Unit = {
    parent ! Done(work)
  }
  
  override val persistenceId: String = s"minion-$work"
  override def journalPluginId: String = "store.mem-journal"

}

object Minion {

  sealed trait Event
  object Event {
    case class Found(depth: Int, urls: Set[Url]) extends Event
    case class Fetched(link: OutLink, found: Found) extends Event
    case class NotFetched(link: OutLink, error: Throwable) extends Event
  }

  case object Next

}