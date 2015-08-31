package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.{AkkaSystem, MocaConfig}
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.MapDBWorkRepo
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.{Success, Failure}

object Moca extends App with StrictLogging {

  logger.info("Moca starting")

  val config = MocaConfig.parse(args)
  
  implicit val system = AkkaSystem.fromConfig(config)

  if (config.hasRole(Master.role)) {
    Master.standBy(new MapDBWorkRepo)
  }

  if (config.hasRole(Worker.role)) {
    (1 to config.workers).foreach(Worker.start)
  }
  
  if (config.hasRole(Client.role)) {
    val commands = Future.sequence {
      val client = Client.start
      config.clientCommands.map { cmd =>
        client ! cmd
        cmd.result.map(r => cmd -> true).recover { case e => cmd -> false }
      }
    }
    commands.onComplete {
      case Success(results) =>
        results.foreach {
          case (cmd, true) => logger.info(s"Command $cmd successful")
          case (cmd, false) => logger.info(s"Command $cmd failed")
        }
      case Failure(t) =>
        logger.error("Failed to run commands", t)
    }
  }

  sys.addShutdownHook {
    logger.info("Moca going down")
    system.terminate()
  }

}



