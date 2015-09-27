package com.github.lucastorri.moca.config

import org.scalatest.{FlatSpec, MustMatchers}

import scala.collection.JavaConversions._

class MocaConfigTest extends FlatSpec with MustMatchers {

  it must "resolve pending configurations" in {

    val port = 7890
    val hostname = "10.11.12.13"
    val seeds = Set("seed1:9123", "seed2:9234")

    val config = MocaConfig()
      .copy(seeds = seeds, hostname = hostname, port = port)

    config.mainConfig.getInt("akka.remote.netty.tcp.port") must equal (port)
    config.mainConfig.getString("akka.remote.netty.tcp.hostname") must equal (hostname)

    config.mainConfig.getStringList("akka.cluster.seed-nodes").toSet must equal (
      Set("akka.tcp://MocaSystem@seed1:9123", "akka.tcp://MocaSystem@seed2:9234"))

  }

}
