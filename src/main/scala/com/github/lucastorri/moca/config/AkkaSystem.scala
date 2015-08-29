package com.github.lucastorri.moca.config

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

object AkkaSystem extends StrictLogging {

  def fromConfig(config: MocaConfig): ActorSystem = {

    val roles = stringArray(config.roles)
    val hostname =
      if (config.hasSeeds) quote(config.hostname)
      else quote("127.0.0.1")
    val seeds =
      if (config.hasSeeds) stringArray(config.seeds.map(hostAndPort => seed(config.systemName, hostAndPort)))
      else stringArray(seed(config.systemName, s"127.0.0.1:${config.port}"))

    val cfg = s"""
      |akka {
      |
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |  }
      |
      |  cluster {
      |    roles = $roles
      |    seed-nodes = $seeds
      |    auto-down-unreachable-after = 10s
      |  }
      |
      |  remote {
      |    netty.tcp {
      |      port = ${config.port}
      |      hostname = $hostname
      |    }
      |  }
      |
      |}
      """.stripMargin

    logger.debug(s"Config: \n$cfg")

    ActorSystem(config.systemName,
      ConfigFactory.parseString(cfg)
        .withFallback(ConfigFactory.parseResourcesAnySyntax("store.conf"))
        .resolve())
  }

  def seed(systemName: String, hostAndPort: String): String =
    s"akka.tcp://$systemName@$hostAndPort"

  def stringArray(strings: Iterable[String]): String =
    strings.map(quote).mkString("[", ",", "]")

  def quote(str: String): String =
    "\"" + str + "\""

  def stringArray(strings: String*): String =
    stringArray(strings)


}
