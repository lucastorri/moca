name := "moca"

version := "0.0.1"

scalaVersion := "2.11.7"

scalacOptions := Seq("-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "com.typesafe.akka" %% "akka-remote" % "2.4-SNAPSHOT",
  "com.typesafe.akka" %% "akka-cluster-sharding" % "2.4-SNAPSHOT",
  "com.typesafe.akka" %% "akka-contrib" % "2.4-SNAPSHOT",
  "com.typesafe.akka" %% "akka-distributed-data-experimental" % "2.4-SNAPSHOT",
  "org.testfx" % "openjfx-monocle" % "1.8.0_20",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.github.crawler-commons" % "crawler-commons" % "0.6",
  "io.mola.galimatias" % "galimatias" % "0.2.0",
  "org.jsoup" % "jsoup" % "1.8.3",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.mapdb" % "mapdb" % "2.0-beta6",
  "com.esotericsoftware" % "kryo" % "3.0.3",
  "com.typesafe.slick" %% "slick" % "3.0.2",
  "com.github.tminglei" %% "slick-pg" % "0.9.1",
  "org.postgresql" % "postgresql" % "9.4-1202-jdbc42",
  "com.zaxxer" % "HikariCP" % "2.4.1",
  "com.okumin" %% "akka-persistence-sql-async" % "0.3" exclude("com.typesafe.akka", "akka-persistence_2.11"),
  "com.github.mauricio" %% "postgresql-async" % "0.2.16",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.10.17")

resolvers ++= Seq(
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/")

mainClass in assembly := Some("com.github.lucastorri.moca.Moca")

assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(prependShellScript = Some(Seq("#!/usr/bin/env sh", "exec java -jar \"$0\" \"$@\"")))

assemblyJarName in assembly := s"${name.value}-${version.value}"

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.github.lucastorri.moca.config")

net.virtualvoid.sbt.graph.Plugin.graphSettings

