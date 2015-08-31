name := "moca"

version := "0.1"

scalaVersion := "2.11.7"

scalacOptions := Seq("-deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-remote" % "2.4.0-RC1",
  "com.typesafe.akka" %% "akka-cluster-sharding" % "2.4.0-RC1",
  "com.typesafe.akka" %% "akka-contrib" % "2.4.0-RC1",
  "com.typesafe.akka" %% "akka-distributed-data-experimental" % "2.4.0-RC1",
  "org.testfx" % "openjfx-monocle" % "1.8.0_20",
  "org.jsoup" % "jsoup" % "1.8.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.mapdb" % "mapdb" % "2.0-beta6")
