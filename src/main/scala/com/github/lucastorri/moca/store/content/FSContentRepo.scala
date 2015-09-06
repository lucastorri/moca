package com.github.lucastorri.moca.store.content

import java.io.{DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.store.content.serializer.ContentSerializer
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class FSContentRepo(config: Config, serializer: ContentSerializer) extends ContentRepo {

  val base = Paths.get(config.getString("directory")).toAbsolutePath
  base.toFile.mkdirs()

  override def apply(task: Task): TaskContentRepo =
    repo(task).addSeeds(task.seeds)

  override def links(task: Task): ContentLinksTransfer =
    FileContentLinksTransfer(repo(task).log.toString)

  private def repo(task: Task): FSTaskContentRepo =
    FSTaskContentRepo(base.resolve(task.id), serializer)

}

case class FSTaskContentRepo(directory: Path, serializer: ContentSerializer) extends TaskContentRepo {

  directory.toFile.mkdirs()
  val charset = StandardCharsets.UTF_8
  val log = directory.resolve("__log")
  lazy val seeds = directory.resolve("__seeds")

  def addSeeds(newSeeds: Set[Url]): TaskContentRepo = {
    val str = newSeeds.mkString("\n")
    Files.write(seeds, s"$str\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    this
  }

  override def save(url: Url, depth: Int, content: Try[Content]): Future[Unit] = {
    val (serialized, hash) = content match {
      case Success(c) => serializer.serialize(url, c) -> c.hash
      case Failure(t) => serializer.serialize(url, t) -> ""
    }
    val file = directory.resolve(url.id)
    Files.write(file, serialized.array(), StandardOpenOption.CREATE)

    val data = new DataOutputStream(new FileOutputStream(log.toFile, true))
    data.writeInt(depth)
    data.writeUTF(url.toString)
    data.writeUTF(file.toString)
    data.writeUTF(hash.toString)
    data.flush()
    data.close()
    Future.successful(())
  }

}

case class FileContentLinksTransfer(log: String) extends ContentLinksTransfer {

  val data = new DataInputStream(new FileInputStream(Paths.get(log).toFile))

  override def contents: Stream[ContentLink] = {
    val data = new DataInputStream(new FileInputStream(Paths.get(log).toFile))
    def next: Stream[ContentLink] = {
      if (data.available() <= 0) Stream.empty
      else Stream {
        val depth = data.readInt()
        val url = Url(data.readUTF())
        val file = data.readUTF()
        val hash = data.readUTF()

        ContentLink(url, file, depth, hash)
      } #::: next
    }
    next
  }

}