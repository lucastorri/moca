package com.github.lucastorri.moca.store.content

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.Future

class FSContentRepo(config: Config) extends ContentRepo {

  val base = Paths.get(config.getString("directory")).toAbsolutePath
  base.toFile.mkdirs()

  override def apply(work: Work): WorkContentRepo =
    repo(work)

  override def links(work: Work): WorkContentTransfer =
    FileWorkContentTransfer(repo(work).log.toString)

  private def repo(work: Work): FSWorkContentRepo =
    FSWorkContentRepo(base.resolve(work.id))

}

case class FSWorkContentRepo(directory: Path) extends WorkContentRepo {

  directory.toFile.mkdirs()
  val log = directory.resolve("__log")

  override def save(url: Url, content: Content): Future[Unit] = {
    val file = directory.resolve(url.id)
    Files.write(file, content.content.array(), StandardOpenOption.CREATE)
    Files.write(log, s"$url|$file\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    Future.successful(())
  }

}

case class FileWorkContentTransfer(log: String) extends WorkContentTransfer {

  override def contents: Stream[ContentLink] = {
    Files.readAllLines(Paths.get(log)).asScala.toStream.map { line =>
      val Array(url, link) = line.split("\\|")
      ContentLink(Url(url), link)
    }
  }

}