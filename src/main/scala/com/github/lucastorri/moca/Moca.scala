package com.github.lucastorri.moca

import com.github.lucastorri.moca.config.{AkkaSystem, MocaConfig}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.InMemWorkRepo

object Moca extends App {

  implicit val system = AkkaSystem.fromConfig(MocaConfig.parse(args))

  Master.join(new InMemWorkRepo)

  //TODO start only if it's not a master: have a different system for them, if is/isn't master, just terminate/start it
  (1 to 2).foreach(Worker.start)

  sys.addShutdownHook {
    system.terminate()
  }

}



