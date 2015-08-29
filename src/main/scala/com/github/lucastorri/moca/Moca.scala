package com.github.lucastorri.moca

import akka.actor.ActorSystem
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.work.InMemWorkRepo
import com.typesafe.config.ConfigFactory

object Moca extends App {

  val systemName = "MocaSystem"

  val config = ConfigFactory.parseString(
    s"""
      |akka.cluster.roles = ["${Master.role}", "${Worker.role}"]
      |akka.cluster.seed-nodes = ["akka.tcp://$systemName@127.0.0.1:${ClusterSeed.port}"]
    """.stripMargin)
    .withFallback(ConfigFactory.parseResourcesAnySyntax("system.conf"))
    .resolve()



  ClusterSeed.start

  implicit val system = ActorSystem(systemName, config)

  Master.join(new InMemWorkRepo)

  //TODO start only if it's not a master: have a different system for them, if is/isn't master, just terminate/start it
  (1 to 2).foreach(Worker.start)

}

object ClusterSeed {

  val port = 8888

  def start: ActorSystem = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
         |
         |akka.remote.netty.tcp.port = $port
         |akka.remote.netty.tcp.hostname = 127.0.0.1
         |
         |akka.cluster.seed-nodes = ["akka.tcp://${Moca.systemName}@127.0.0.1:$port"]
         |akka.cluster.auto-down-unreachable-after = 10s
      """.stripMargin)

    val system = ActorSystem(Moca.systemName, config)

    system
  }

}