package com.github.lucastorri.moca.config

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging

object AkkaSystem extends StrictLogging {

  def fromConfig(config: MocaConfig): ActorSystem = {

    val roles = stringArray(config.roles)
    val hostname =
      if (config.isNotSingleInstance) quote(config.hostname)
      else quote("127.0.0.1")
    val seeds =
      if (config.isNotSingleInstance) stringArray(config.seeds.map(hostAndPort => seed(config.systemName, hostAndPort)))
      else stringArray(seed(config.systemName, s"127.0.0.1:${config.port}"))

    val extraConfig = config.extraConfig match {
      case Some(f) => ConfigFactory.parseFile(f)
      case None => ConfigFactory.parseString("")
    }

    val resolve = ConfigFactory.parseString(s"""
        |resolve {
        |  roles = $roles
        |  seeds = $seeds
        |  port = ${config.port}
        |  host = $hostname
        |}
      """.stripMargin)

    val mainConfig = ConfigFactory.parseResourcesAnySyntax("main.conf")

    val systemConfig = extraConfig
        .withFallback(mainConfig)
        .withFallback(resolve)
        .resolve()

    logger.debug(s"Config: \n$systemConfig")

    ActorSystem(config.systemName, systemConfig)
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
