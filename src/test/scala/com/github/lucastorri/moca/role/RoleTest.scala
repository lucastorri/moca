package com.github.lucastorri.moca.role

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait RoleTest extends BeforeAndAfterAll { self: Suite =>

  def port: Int
  def config: String

  final val name = this.getClass.getSimpleName
  implicit val timeout: Timeout = 5.seconds
  implicit lazy val system = {
    val baseConfig = ConfigFactory.parseString(
      s"""
         |akka {
         |
         | actor {
         |   provider = "akka.cluster.ClusterActorRefProvider"
         | }
         |
         | cluster {
         |   roles = ["master"]
         |   seed-nodes = ["akka.tcp://$name@127.0.0.1:$port"]
         |   auto-down-unreachable-after = 10s
         | }
         |
         | remote {
         |   netty.tcp {
         |     port = $port
         |     hostname = "127.0.0.1"
         |   }
         | }
         |
         |}
    """.stripMargin)
    ActorSystem(name, baseConfig.withFallback(ConfigFactory.parseString(config)).resolve())
  }

  override protected def afterAll(): Unit = {
    system.terminate()
  }

  def result[R](f: Future[R]): R =
    Await.result(f, timeout.duration)

}
