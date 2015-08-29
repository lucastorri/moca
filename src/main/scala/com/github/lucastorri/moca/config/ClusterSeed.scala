package com.github.lucastorri.moca.config

import akka.actor.ActorSystem
import com.github.lucastorri.moca.Moca
import com.typesafe.config.ConfigFactory

object ClusterSeed {

  def start(name: String, port: Int): ActorSystem = {
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
         |
         |akka.remote.netty.tcp.port = $port
         |akka.remote.netty.tcp.hostname = 127.0.0.1
         |
         |akka.cluster.seed-nodes = ["akka.tcp://$name@127.0.0.1:$port"]
         |akka.cluster.auto-down-unreachable-after = 10s
      """.stripMargin)

    ActorSystem(name, config)
  }

}
