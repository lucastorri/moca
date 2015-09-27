package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.MocaConfig
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.typesafe.scalalogging.StrictLogging

import scala.util.{Failure, Success}

object Moca extends App with StrictLogging {

  val config = MocaConfig.parse(args)
  implicit val system = config.system
  implicit val exec = system.dispatcher

  logger.info("Moca starting")

  if (config.hasRole(Worker.role)) {
    val start = Worker.start(config.contentRepo, config.browserProvider, config.partition, config.dedicatedMaster) _
    (1 to config.workers).foreach(start)
  }

  if (config.hasRole(Master.role)) {
    Master.standBy(config.runControl)
  }
  
  if (config.hasRole(Client.role)) {
    Client.run(config.clientCommands).onComplete {
      case Success(results) => results.foreach(println); system.terminate()
      case Failure(t) => logger.error("Failed to run commands", t); System.exit(1)
    }
  }

  sys.addShutdownHook {
    logger.info("Moca going down")
    system.terminate()
  }

}



