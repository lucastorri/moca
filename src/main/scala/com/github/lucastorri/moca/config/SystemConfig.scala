package com.github.lucastorri.moca.config

import com.typesafe.config.{Config, ConfigFactory}

object SystemConfig {

  def fromConfig(config: MocaConfig): Config = {

    val hostname = quote(config.hostname)
    val roles = stringArray(config.roles)
    val seeds =
      if (config.hasSeeds) stringArray(config.seeds.map(hostAndPort => seed(config.systemName, hostAndPort)))
      else stringArray(seed(config.systemName, s"127.0.0.1:${config.singletonPort}"))

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

    ConfigFactory.parseString(cfg)
      .withFallback(ConfigFactory.parseResourcesAnySyntax("store.conf"))
      .resolve()
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
