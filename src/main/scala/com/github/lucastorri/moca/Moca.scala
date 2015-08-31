package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.{AkkaSystem, MocaConfig}
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.MapDBWorkRepo
import com.typesafe.scalalogging.StrictLogging

import scala.util.{Failure, Success}

object Moca extends App with StrictLogging {

  logger.info("Moca starting")
  val config = MocaConfig.parse(args)
  implicit val system = AkkaSystem.fromConfig(config)
  implicit val exec = system.dispatcher

  if (config.hasRole(Master.role)) {
    Master.standBy(new MapDBWorkRepo)
  }

  if (config.hasRole(Worker.role)) {
    (1 to config.workers).foreach(Worker.start)
  }
  
  if (config.hasRole(Client.role)) {
    Client.run(config.clientCommands).onComplete {
      case Success(results) =>
        results.foreach { case (cmd, r) => logger.info(s"Command $cmd ${if (r) "successful" else "failed"}") }
        system.terminate()
      case Failure(t) =>
        logger.error("Failed to run commands", t)
        System.exit(1)
    }
  }

  sys.addShutdownHook {
    logger.info("Moca going down")
    system.terminate()
  }

}



