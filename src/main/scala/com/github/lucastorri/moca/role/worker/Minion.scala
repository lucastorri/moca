package com.github.lucastorri.moca.role.worker

import akka.persistence.{PersistentActor, RecoveryCompleted}
import com.github.lucastorri.moca.browser.{Browser, Content}
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Minion.Event.{Fetched, Found, NotFetched}
import com.github.lucastorri.moca.role.worker.Minion.{Event, Next}
import com.github.lucastorri.moca.role.worker.Worker.{Done, Partition}
import com.github.lucastorri.moca.store.content.TaskContentRepo
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

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

      case NotFetched(url, _) =>
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
        val o = outstanding.head
        val result =
          for {
            (fetched, content) <- fetch(o)
            _ <- repo.save(o.url, o.depth, content)
          } yield fetched
        result.onComplete {
          case Success(fetched) =>
            self ! fetched
          case Failure(t) =>
            self ! NotFetched(o, t)
        }
      }

    case e: Event => persist(e) {

      case found: Found =>
        addToQueue(found)

      case fetched @ Fetched(url, found) =>
        logger.debug(s"Fetched $url")
        markFetched(url)
        addToQueue(found)
        scheduleNext()

      case notFetched @ NotFetched(url, t) =>
        logger.error(s"Could not fetch $url", t)
        markFetched(url)
        scheduleNext()

      }

  }

  def fetch(link: Link): Future[(Fetched, Content)] = {
    logger.trace(s"Requesting ${link.url}")
    browser.goTo(link.url) { page =>
      logger.trace(s"Processing ${link.url}")
      Fetched(link, Found(link.depth + 1, task.select(link, page))) -> page.content
    }
  }

  def addToQueue(found: Found): Unit = {
    val (toAdd, toFwd) = Link.all(found)
      .filter(l => !downloaded.contains(l.url.hashCode))
      .partition(link => partition.same(task, link.url))

    toFwd.foreach(markFetched)
    if (toFwd.nonEmpty) parent ! Partition(toFwd)

    toAdd.foreach(l => if (!outstanding.contains(l)) outstanding += l)
  }

  def markFetched(link: Link): Unit = {
    outstanding.remove(link)
    downloaded += link.url.hashCode
  }

  def scheduleNext(): Unit = {
    system.scheduler.scheduleOnce(task.intervalBetweenRequests, self, Next)
  }

  def finish(): Unit = {
    deleteMessages(lastSequenceNr)
    parent ! Done
  }

  override def unhandled(message: Any): Unit = message match {
    case _ => logger.error(s"Unknown message $message")
  }

  override val persistenceId: String = s"minion-${task.id}"
  override def journalPluginId: String = system.settings.config.getString("moca.minion.journal-plugin-id")

}

object Minion {

  sealed trait Event
  object Event {
    case class Found(depth: Int, urls: Set[Url]) extends Event
    case class Fetched(link: Link, found: Found) extends Event
    case class NotFetched(link: Link, error: Throwable) extends Event
  }

  case object Next

}