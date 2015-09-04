package com.github.lucastorri.moca.store.content

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.Future

class FSContentRepo(config: Config) extends ContentRepo {

  val base = Paths.get(config.getString("directory")).toAbsolutePath
  base.toFile.mkdirs()

  override def apply(task: Task): TaskContentRepo =
    repo(task).addSeeds(task.seeds)

  override def links(task: Task): ContentLinksTransfer =
    FileContentLinksTransfer(repo(task).log.toString)

  private def repo(task: Task): FSTaskContentRepo =
    FSTaskContentRepo(base.resolve(task.id))

}

case class FSTaskContentRepo(directory: Path) extends TaskContentRepo {

  directory.toFile.mkdirs()
  val log = directory.resolve("__log")
  lazy val seeds = directory.resolve("__seeds")

  def addSeeds(newSeeds: Set[Url]): TaskContentRepo = {
    val str = newSeeds.mkString("\n")
    Files.write(seeds, s"$str\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    this
  }

  override def save(url: Url, depth: Int, content: Content): Future[Unit] = {
    //TODO save status and headers (decide a format, maybe json)
    val file = directory.resolve(url.id)
    Files.write(file, content.content.array(), StandardOpenOption.CREATE)
    val logEntry = s"$url|$file|$depth|${content.hash}\n".getBytes(StandardCharsets.UTF_8)
    Files.write(log, logEntry, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    Future.successful(())
  }

}

case class FileContentLinksTransfer(log: String) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = {
    Files.readAllLines(Paths.get(log)).asScala.toStream.map { line =>
      val Array(url, link, depth, hash) = line.split("\\|")
      ContentLink(Url(url), link, depth.toInt, hash)
    }
  }

}