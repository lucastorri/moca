package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.{AkkaSystem, MocaConfig}
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.MapDBWorkRepo

object Moca extends App {

  val config = MocaConfig.parse(args)
  
  implicit val system = AkkaSystem.fromConfig(config)

  if (config.hasRole(Master.role)) {
    Master.standBy(new MapDBWorkRepo)
  }

  if (config.hasRole(Worker.role)) {
    (1 to config.workers).foreach(Worker.start)
  }
  
  if (config.hasRole(Client.role)) {
    val client = Client.start
    config.clientCommands.foreach(client ! _)
  }

  sys.addShutdownHook {
    system.terminate()
  }

}



