package com.github.lucastorri.moca.role.worker

import akka.pattern.ask
import akka.persistence.{DeleteMessagesSuccess, PersistentActor, RecoveryCompleted}
import akka.util.Timeout
import com.github.lucastorri.moca.browser.{Browser, Content}
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Minion.Event.{Fetched, Found, NotFetched}
import com.github.lucastorri.moca.role.worker.Minion.{Event, Next}
import com.github.lucastorri.moca.role.worker.Worker.{Continue, Done, Partition}
import com.github.lucastorri.moca.store.content.TaskContentRepo
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class Minion(task: Task, browser: Browser, repo: TaskContentRepo, partition: PartitionSelector) extends PersistentActor with StrictLogging {

  import context._

  private val downloaded = mutable.HashSet.empty[Int]
  private val outstanding = mutable.LinkedHashSet.empty[Link]

  override def preStart(): Unit = {
    logger.trace(s"Minion started to work on ${task.id}")
  }

  override def receiveRecover: Receive = {

    case e: Event => e match {

      case found: Found =>
        addToQueue(found)

      case Fetched(url, found) =>
        markFetched(url)
        addToQueue(found)

      case NotFetched(url) =>
        markFetched(url)

    }

    case RecoveryCompleted =>
      if (downloaded.isEmpty) self ! Found(task.initialDepth, task.seeds)
      self ! Next

  }

  override def receiveCommand: Receive = {

    case Next =>
      if (outstanding.isEmpty) finish()
      else {
        val next = outstanding.head

        val saved =
          for {
            result <- fetch(next)
            saved <- save(next, result)
          } yield saved

        saved.onComplete {
          case Success(Some(fetched)) =>
            self ! fetched
          case Success(None) =>
            self ! NotFetched(next)
          case Failure(t) =>
            logger.error(s"Could not save ${next.url}", t)
            self ! NotFetched(next)
        }
      }

    case e: Event => persist(e) {

      case found: Found =>
        addToQueue(found)

      case fetched @ Fetched(url, found) =>
        markFetched(url)
        addToQueue(found)
        scheduleNext()

      case notFetched @ NotFetched(url) =>
        markFetched(url)
        scheduleNext()

      }

  }

  def fetch(link: Link): Future[Try[(Fetched, Content)]] = {
    logger.trace(s"Requesting ${link.url}")
    val result = browser.goTo(link.url) { page =>
      logger.trace(s"Processing ${link.url}")
      Fetched(link, Found(link.depth + 1, task.select(link, page))) -> page.renderedContent
    }
    result.map(Success.apply).recover { case e => Failure(e) }
  }

  def save(link: Link, result: Try[(Fetched, Content)]): Future[Option[Fetched]] = {
    //TODO save both original and rendered Url
    result match {
      case Success((fetched, content)) =>
        logger.debug(s"Fetched ${fetched.link.url} at depth ${fetched.link.depth}")
        repo.save(link.url, link.depth, content).map(_ => Some(fetched))
      case Failure(t) =>
        logger.debug(s"Could not fetch ${link.url}", t)
        repo.save(link.url, link.depth, t).map(_ => None)
    }
  }

  def addToQueue(found: Found): Unit = {
    val inAnotherPartition = found.urls
      .filter(url => !downloaded.contains(url.hashCode))
      .flatMap { url =>
        val link = Link(url, found.depth)
        if (partition.same(task, url)) {
          if (!outstanding.contains(link)) outstanding += link
          None
        } else {
          markFetched(link)
          Some(url)
        }
      }

    if (inAnotherPartition.nonEmpty) {
      notify(Partition(inAnotherPartition, found.depth))
    }
  }

  def markFetched(link: Link): Unit = {
    outstanding.remove(link)
    downloaded += link.url.hashCode
  }

  def scheduleNext(): Unit = {
    system.scheduler.scheduleOnce(task.intervalBetweenRequests, self, Next)
  }

  def finish(): Unit = {
    notify(Done)
    deleteMessages(lastSequenceNr)
  }

  override def unhandled(message: Any): Unit = message match {
    case d: DeleteMessagesSuccess =>
    case _ => logger.error(s"Unknown message $message")
  }

  private def notify(msg: Any): Unit = {
    implicit val timeout: Timeout = 1.hour
    Try(Await.result(parent ? msg, timeout.duration)) match {
      case Success(Continue) =>
      case other =>
        context.stop(self)
        throw other match {
          case Success(r) => AbortException(task, r)
          case Failure(t) => AbortException(task, "-none-", t)
        }
    }
  }

  override val persistenceId: String = s"minion-${task.id}"
  override def journalPluginId: String = system.settings.config.getString("moca.minion.journal-plugin-id")

}

case class AbortException(task: Task, reply: Any, cause: Throwable = null)
  extends Exception(s"Aborting task ${task.id} after reply $reply from worker", cause)

object Minion {

  sealed trait Event
  object Event {
    case class Found(depth: Int, urls: Set[Url]) extends Event
    case class Fetched(link: Link, found: Found) extends Event
    case class NotFetched(link: Link) extends Event
  }

  case object Next

}