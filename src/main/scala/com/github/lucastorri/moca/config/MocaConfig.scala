package com.github.lucastorri.moca.config

import java.io.File
import java.nio.charset.Charset

import akka.actor.ActorSystem
import com.github.lucastorri.moca.browser.{BrowserProvider, BrowserSettings}
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.client.Client.Command.{AddSeedFile, CheckWorkRepoConsistency, GetSeedResults}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.content.ContentRepo
import com.github.lucastorri.moca.store.content.serializer.ContentSerializer
import com.github.lucastorri.moca.store.control.RunControl
import com.github.lucastorri.moca.store.serialization.SerializerService
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import MocaConfig.seedToAkkaSeed

case class MocaConfig(
  systemName: String = "MocaSystem",
  seeds: Set[String] = Set.empty,
  port: Int = 1731,
  hostname: String = "",
  workers: Int = 10,
  clientCommands: Set[Client.Command[_]] = Set.empty,
  extraConfig: Option[File] = Option.empty,
  dedicatedMaster: Boolean = false,
  private var _roles: Set[String] = Set(Master.role, Worker.role)
) {

  val roles: Set[String] =
    if (clientCommands.nonEmpty) Set(Client.role) else _roles

  def isNotSingleInstance: Boolean =
    seeds.nonEmpty

  def hasRole(role: String): Boolean =
    roles.contains(role)

  lazy val mainConfig: Config = {
    
    val extraCfg = extraConfig match {
      case Some(f) => ConfigFactory.parseFile(f)
      case None => ConfigFactory.parseString("")
    }

    val host = if (isNotSingleInstance) hostname else "127.0.0.1"
    val akkaSeeds =
      if (isNotSingleInstance) seeds.map(seed => seedToAkkaSeed(systemName, seed))
      else Set(seedToAkkaSeed(systemName, s"127.0.0.1:$port"))

    val resolveCfg = ConfigFactory.empty()
      .withValue("resolve.roles", ConfigValueFactory.fromIterable(roles))
      .withValue("resolve.seeds", ConfigValueFactory.fromIterable(akkaSeeds))
      .withValue("resolve.port", ConfigValueFactory.fromAnyRef(port))
      .withValue("resolve.host", ConfigValueFactory.fromAnyRef(host))

    val mainCfg = ConfigFactory.parseResourcesAnySyntax("main.conf")

    extraCfg
      .withFallback(mainCfg)
      .withFallback(resolveCfg)
      .resolve()
  }

  lazy val system: ActorSystem =
    ActorSystem(systemName, mainConfig)


  def runControl: RunControl = {
    val controlConfig = mainConfig.getConfig(mainConfig.getString("moca.run-control-id"))
    val build = ClassBuilder.fromConfig(controlConfig,
      classOf[ExecutionContext] -> system.dispatcher,
      classOf[PartitionSelector] -> partition,
      classOf[SerializerService] -> serializerService)

    build()
  }

  lazy val contentSerializer: ContentSerializer = {
    val serializerConfig = mainConfig.getConfig(mainConfig.getString("moca.content-serializer-id"))
    val build = ClassBuilder.fromConfig(serializerConfig)

    build()
  }

  def contentRepo: ContentRepo = {
    val repoConfig = mainConfig.getConfig(mainConfig.getString("moca.content-repo-id"))
    val build = ClassBuilder.fromConfig(repoConfig,
      classOf[ActorSystem] -> system,
      classOf[ContentSerializer] -> contentSerializer)

    build()
  }
  
  lazy val partition: PartitionSelector = {
    val partitionConfig = mainConfig.getConfig(mainConfig.getString("moca.partition-selector-id"))
    val build = ClassBuilder.fromConfig(partitionConfig)

    build()
  }

  lazy val browserProvider: BrowserProvider = {
    val providerConfig = mainConfig.getConfig(mainConfig.getString("moca.browser-provider-id"))
    val build = ClassBuilder.fromConfig(providerConfig,
      classOf[ActorSystem] -> system,
      classOf[ExecutionContext] -> system.dispatcher,
      classOf[BrowserSettings] -> browserSettings)

    build()
  }

  lazy val browserSettings: BrowserSettings = {
    val baseConfig = mainConfig.getConfig("browser")
    BrowserSettings(
      Charset.forName(baseConfig.getString("html-charset")),
      Duration.fromNanos(baseConfig.getDuration("load-timeout").toNanos),
      baseConfig.getString("user-agent"))
  }
  
  lazy val serializerService: SerializerService = {
    val serviceConfig = mainConfig.getConfig(mainConfig.getString("moca.serializer-service-id"))
    val build = ClassBuilder.fromConfig(serviceConfig,
      classOf[ActorSystem] -> system)

    build()
  }

}

object MocaConfig {

  def parse(args: Array[String]): MocaConfig =
    parser.parse(args, MocaConfig()).getOrElse(sys.exit(1))

  private val parser = new scopt.OptionParser[MocaConfig](BuildInfo.name) {

    head(BuildInfo.name, BuildInfo.version)

    opt[String]('n', "name")
      .text("name of system for all members in cluster")
      .action { (n, c) => c.copy(systemName = n) }

    opt[String]('S', "cluster-seeds")
      .unbounded()
      .valueName("host:port")
      .text("jars to include")
      .validate(s => if (s.matches(".*:\\d+")) success else failure(s"invalid format $s"))
      .action { (s, c) => c.copy(seeds = c.seeds + s) }

    opt[File]('s', "seeds")
      .text("url seeds file to be added")
      .validate(f => if (f.isFile) success else failure(s"invalid file $f"))
      .action { (f, c) => c.copy(clientCommands = c.clientCommands + AddSeedFile(f)) }

    opt[Unit]("check-repo")
      .text("check if any work marked as in progress is still happening")
      .action { (_, c) => c.copy(clientCommands = c.clientCommands + CheckWorkRepoConsistency()) }

    opt[String]('r', "results-for")
      .valueName("seed-id")
      .text("get results for a given seed")
      .action { (r, c) => c.copy(clientCommands = c.clientCommands + GetSeedResults(r)) }

    opt[Int]('p', "port")
      .text("main system port")
      .action { (p, c) => c.copy(port = p) }

    opt[String]('h', "hostname")
      .text("main system hostname")
      .action { (h, c) => c.copy(hostname = h) }

    opt[Int]('w', "workers")
      .text("number of workers to be spawned")
      .action { (w, c) => c.copy(workers = w) }

    opt[File]('c', "config")
      .text("extra conf file")
      .validate(f => if (f.isFile) success else failure(s"invalid file $f"))
      .action { (f, c) => c.copy(extraConfig = Some(f)) }

    opt[Unit]('d', "dedicated-master")
      .text("indicate that master should be the only thing running on a given system")
      .action { (d, c) => c.copy(dedicatedMaster = true) }

    help("help")
      .text("prints this usage text")

  }

  def seedToAkkaSeed(systemName: String, hostAndPort: String): String =
    s"akka.tcp://$systemName@$hostAndPort"

}
