package com.github.lucastorri.moca.role.client

import java.io.File
import java.security.MessageDigest

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.github.lucastorri.moca.async.retry
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.client.Client.Command.{AddSeedFile, CheckWorkRepoConsistency, GetSeedResults}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.store.content.WorkContentTransfer
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
      logger.info("Sending check request")
      (master ? ConsistencyCheck).acked.map(_ => ())
    }

    case get @ GetSeedResults(workId) => get.reply {
      logger.info(s"Requesting results for $workId")
      (master ? GetLinks(workId)).mapTo[WorkLinks].map(_.transfer.get)
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

  override def unhandled(message: Any): Unit = {
    logger.error(s"Unhandled message $message")
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

  def run(commands: Set[Command[_]])(implicit system: ActorSystem, exec: ExecutionContext): Future[Set[CommandResult[_]]] = {
    val client = system.actorOf(Props[Client])
    Future.sequence {
      commands.map { cmd =>
        client ! cmd
        cmd.result
      }
    }
  }
  
  sealed abstract class Command[T](convert: T => String) {

    private[this] val promise = Promise[CommandResult[T]]()

    private[Client] def reply(f: => Future[T])(implicit exec: ExecutionContext): Unit = {
      Try(f) match {
        case Success(s) => s.onComplete { case r => promise.success(CommandResult(this, r)(convert)) }
        case Failure(t) => promise.failure(t)
      }
    }

    def result: Future[CommandResult[T]] = promise.future

  }

  object Command {
    case class AddSeedFile(file: File) extends Command[Unit](_ => "success")
    case class CheckWorkRepoConsistency() extends Command[Unit](_ => "success")
    case class GetSeedResults(seedId: String) extends Command[WorkContentTransfer](_.contents.mkString("\n"))
  }

  case class CommandResult[T](cmd: Command[T], result: Try[T])(str: T => String) {

    private def repr: String = result match {
      case Success(r) => str(r)
      case Failure(t) => s"${t.getClass.getName}(${t.getMessage})"
    }

    override def toString: String = s"$cmd: $repr"
  }

}

