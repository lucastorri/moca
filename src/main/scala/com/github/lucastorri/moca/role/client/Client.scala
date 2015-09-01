package com.github.lucastorri.moca.role.client

import java.io.File
import java.security.MessageDigest

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.client.Client.Command.{AddSeedFile, CheckWorkRepoConsistency}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.Source
import scala.util.{Failure, Success, Try}

class Client extends Actor with StrictLogging {

  import context._

  val master = Master.proxy()
  implicit val timeout: Timeout = 15.seconds

  override def receive: Receive = {

    case add @ AddSeedFile(file) => add.reply {
      logger.info(s"Adding seeds from $file")
      val batch = AddBatch(file)
      self ! batch
      batch.promise.future
    }

    case check @ CheckWorkRepoConsistency() => check.reply {
      (master ? ConsistencyCheck).map(_ => ())
    }

    case batch: AddBatch =>
      if (batch.isEmpty) {
        logger.info(s"Successfully added seeds from ${batch.file}")
        batch.promise.success(())
      } else {
        val next = batch.next.map(url => Work(id(url), Url(url)))
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
  
  val role = "client"

  def run(commands: Set[Command])(implicit system: ActorSystem, exec: ExecutionContext): Future[Set[(Command, Boolean)]] = {
    val client = system.actorOf(Props[Client])
    Future.sequence {
      commands.map { cmd =>
        client ! cmd
        cmd.result.map(r => cmd -> true).recover { case e => cmd -> false }
      }
    }
  }
  
  sealed trait Command {

    private[Client] val promise = Promise[Unit]()

    private[Client] def reply(f: => Future[Unit]): Unit =
      Try(f) match {
        case Success(r) => promise.completeWith(r)
        case Failure(t) => promise.failure(t)
      }

    def result: Future[Unit] = promise.future

  }

  object Command {
    case class AddSeedFile(file: File) extends Command
    case class CheckWorkRepoConsistency() extends Command
  }

}

