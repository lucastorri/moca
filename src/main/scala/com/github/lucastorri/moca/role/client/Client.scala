package com.github.lucastorri.moca.role.client

import java.io.File
import java.security.MessageDigest

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.client.Client.Command.AddSeedFile
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.url.{Seed, Url}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Client extends Actor with StrictLogging {

  import context._

  val master = Master.proxy()
  implicit val timeout: Timeout = 15.seconds

  override def receive: Receive = {

    case add @ AddSeedFile(file) => add.reply {
      val batch = AddBatch(file)
      self ! batch
      batch.promise.future
    }

    case batch: AddBatch =>
      if (batch.isEmpty) {
        logger.info(s"Finished adding ${batch.file}")
        batch.promise.success(())
      } else {
        val next = batch.next.map(url => Seed(id(url), Url(url)))
        retry(3)(master ? AddSeeds(next)).acked.onComplete {
          case Success(_) =>
            logger.info(s"Added ${batch.processed}/${batch.total} of ${batch.file}")
            self ! batch
          case Failure(t) =>
            logger.error(s"Failed to add ${batch.file}", t)
            batch.promise.failure(t)
        }
      }

  }

  case class AddBatch(file: File) {

    private var groups = {
      val source = Source.fromFile(file)
      val seeds = source.getLines().toSet
      source.close()
      seeds.grouped(50).toSeq
    }

    val total = groups.size

    def processed = total - groups.size

    val promise = Promise[Unit]()
    
    def isEmpty: Boolean = groups.isEmpty
    
    def next: Set[String] = {
      val next = groups.head
      groups = groups.tail
      next
    }

  }

  def id(str: String): String =
    MessageDigest.getInstance("SHA1")
      .digest(str.getBytes)
      .map("%02x".format(_))
      .mkString

}

object Client {

  sealed trait Command[T] {

    private[Client] val promise = Promise[T]()

    private[Client] def reply(f: => Future[T]): Unit =
      Try(f) match {
        case Success(r) => promise.completeWith(r)
        case Failure(t) => promise.failure(t)
      }

    def result: Future[T] = promise.future

  }

  object Command {
    case class AddSeedFile(file: File) extends Command[Unit]
  }

}

