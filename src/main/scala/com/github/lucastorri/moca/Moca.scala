package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.{AkkaSystem, MocaConfig}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.InMemWorkRepo

object Moca extends App {

  val config = MocaConfig.parse(args)
  
  implicit val system = AkkaSystem.fromConfig(config)

  if (config.hasRole(Master.role)) {
    Master.join(new InMemWorkRepo)
  }

  if (config.hasRole(Worker.role)) {
    (1 to config.workers).foreach(Worker.start)
  }

  sys.addShutdownHook {
    system.terminate()
  }

}



