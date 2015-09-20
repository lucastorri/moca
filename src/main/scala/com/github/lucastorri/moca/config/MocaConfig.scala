package com.github.lucastorri.moca.config

import java.io.File
import java.nio.charset.Charset

import akka.actor.ActorSystem
import com.github.lucastorri.moca.browser.{BrowserProvider, BrowserSettings}
import com.github.lucastorri.moca.config.MocaConfig._
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.client.Client
import com.github.lucastorri.moca.role.client.Client.Command.{AddSeedFile, CheckWorkRepoConsistency, GetSeedResults}
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker
import com.github.lucastorri.moca.store.content.ContentRepo
import com.github.lucastorri.moca.store.content.serializer.ContentSerializer
import com.github.lucastorri.moca.store.serialization.SerializerService
import com.github.lucastorri.moca.store.work.WorkRepo
import com.github.lucastorri.moca.wip.{RunControl, TaskPublisher}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

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

  lazy val main: Config = {
    val rolesArray = stringArray(roles)
    val hostnameString =
      if (isNotSingleInstance) quote(hostname)
      else quote("127.0.0.1")
    val seedArray =
      if (isNotSingleInstance) stringArray(seeds.map(hostAndPort => seed(systemName, hostAndPort)))
      else stringArray(seed(systemName, s"127.0.0.1:$port"))

    val extraCfg = extraConfig match {
      case Some(f) => ConfigFactory.parseFile(f)
      case None => ConfigFactory.parseString("")
    }

    val resolveCfg = ConfigFactory.parseString(s"""
        |resolve {
        |  roles = $rolesArray
        |  seeds = $seedArray
        |  port = $port
        |  host = $hostnameString
        |}
      """.stripMargin)

    val mainCfg = ConfigFactory.parseResourcesAnySyntax("main.conf")

    extraCfg
      .withFallback(mainCfg)
      .withFallback(resolveCfg)
      .resolve()
  }

  lazy val system: ActorSystem =
    ActorSystem(systemName, main)

  def workRepo: WorkRepo = {
    val repoConfig = main.getConfig(main.getString("moca.work-repo-id"))
    val build = ClassBuilder.fromConfig(repoConfig,
      classOf[ActorSystem] -> system,
      classOf[PartitionSelector] -> partition,
      classOf[SerializerService] -> serializerService,
      classOf[EventBus] -> bus)

    build()
  }

  lazy val taskPublisher: TaskPublisher = {
    new TaskPublisher {
      override def push(tasks: Set[Task]): Future[Unit] = {
        tasks.foreach(task => bus.publish(EventBus.NewTasks, task))
        Future.successful(())
      }
    }
  }

  def runControl: RunControl = {
    val controlConfig = main.getConfig(main.getString("moca.run-control-id"))
    val build = ClassBuilder.fromConfig(controlConfig,
      classOf[ExecutionContext] -> system.dispatcher,
      classOf[PartitionSelector] -> partition,
      classOf[SerializerService] -> serializerService,
      classOf[TaskPublisher] -> taskPublisher,
      classOf[EventBus] -> bus)

    build()
  }

  lazy val contentSerializer: ContentSerializer = {
    val serializerConfig = main.getConfig(main.getString("moca.content-serializer-id"))
    val build = ClassBuilder.fromConfig(serializerConfig)

    build()
  }

  def contentRepo: ContentRepo = {
    val repoConfig = main.getConfig(main.getString("moca.content-repo-id"))
    val build = ClassBuilder.fromConfig(repoConfig,
      classOf[ActorSystem] -> system,
      classOf[ContentSerializer] -> contentSerializer)

    build()
  }
  
  lazy val partition: PartitionSelector = {
    val partitionConfig = main.getConfig(main.getString("moca.partition-selector-id"))
    val build = ClassBuilder.fromConfig(partitionConfig)

    build()
  }

  lazy val browserProvider: BrowserProvider = {
    val providerConfig = main.getConfig(main.getString("moca.browser-provider-id"))
    val build = ClassBuilder.fromConfig(providerConfig,
      classOf[ActorSystem] -> system,
      classOf[ExecutionContext] -> system.dispatcher,
      classOf[BrowserSettings] -> browserSettings)

    build()
  }

  lazy val browserSettings: BrowserSettings = {
    val baseConfig = main.getConfig("browser")
    BrowserSettings(
      Charset.forName(baseConfig.getString("html-charset")),
      Duration.fromNanos(baseConfig.getDuration("load-timeout").toNanos),
      baseConfig.getString("user-agent"))
  }

  lazy val bus: EventBus = {
    val busConfig = main.getConfig(main.getString("moca.event-bus-id"))
    val build = ClassBuilder.fromConfig(busConfig)

    build()
  }
  
  lazy val serializerService: SerializerService = {
    val serviceConfig = main.getConfig(main.getString("moca.serializer-service-id"))
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

  private def seed(systemName: String, hostAndPort: String): String =
    s"akka.tcp://$systemName@$hostAndPort"

  private def stringArray(strings: Iterable[String]): String =
    strings.map(quote).mkString("[", ",", "]")

  private def quote(str: String): String =
    "\"" + str + "\""

  private def stringArray(strings: String*): String =
    stringArray(strings)

}
