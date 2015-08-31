package com.github.lucastorri.moca.config

import java.io.File

import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.worker.Worker

case class MocaConfig(
  systemName: String = "MocaSystem",
  seeds: Set[String] = Set.empty,
  roles: Set[String] = MocaConfig.availableRoles,
  port: Int = 1731,
  hostname: String = "",
  workers: Int = 10,
  extraConfig: Option[File] = Option.empty
) {

  def isNotSingleInstance: Boolean =
    seeds.nonEmpty

  def hasRole(role: String): Boolean =
    roles.contains(role)

}

object MocaConfig {

  //TODO use generator
  val v = "0.0.1"
  val name = "moca"

  val availableRoles = Set(Master.role, Worker.role)

  private val parser = new scopt.OptionParser[MocaConfig](name) {

    head(name, v)

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
      .action { (f, c) => /*TODO*/ ??? }

    opt[Int]('p', "port")
      .text("main system port")
      .action { (p, c) => c.copy(port = p) }

    opt[String]('h', "hostname")
      .text("main system hostname")
      .action { (h, c) => c.copy(hostname = h) }

    opt[Seq[String]]('R', "roles")
      .text("roles of this instance")
      .validate(_.find(r => !availableRoles.contains(r)).map(r => failure(s"unknown role $r")).getOrElse(success))
      .action { (r, c) => c.copy(roles = r.toSet) }

    opt[Unit]("print-roles")
      .text("list roles available and exit")
      .action { (_, c) => println(availableRoles); sys.exit(0) }

    opt[Int]('w', "workers")
      .text("number of workers to be spawned")
      .action { (w, c) => c.copy(workers = w) }

    opt[File]('c', "config")
      .text("extra conf file")
      .validate(f => if (f.isFile) success else failure(s"invalid file $f"))
      .action { (f, c) => c.copy(extraConfig = Some(f)) }

  }

  def parse(args: Array[String]): MocaConfig =
    parser.parse(args, MocaConfig()).getOrElse(sys.exit(1))

}
