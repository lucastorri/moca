package com.github.lucastorri.moca

import akka.actor.ActorSystem
import com.github.lucastorri.moca.config.{ClusterSeed, MocaConfig, SystemConfig}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.InMemWorkRepo

object Moca extends App {

  val config = MocaConfig.parse(args)

  val singletonSystem =
    if (config.hasSeeds) None
    else Some(ClusterSeed.start(config.systemName, config.singletonPort))

  implicit val system =
    ActorSystem(config.systemName, SystemConfig.fromConfig(config))

  Master.join(new InMemWorkRepo)

  //TODO start only if it's not a master: have a different system for them, if is/isn't master, just terminate/start it
  (1 to 2).foreach(Worker.start)

  sys.addShutdownHook {
    system.terminate()
    singletonSystem.foreach(_.terminate())
  }

}



