package com.github.lucastorri.moca.store.work

import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.store.content.{ContentLink, WorkContentTransfer}
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.typesafe.config.{Config, ConfigMemorySize}
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

class MapDBWorkRepo(base: Path, increment: ConfigMemorySize, system: ActorSystem) extends WorkRepo with StrictLogging {

  def this(config: Config, system: ActorSystem) = this(
    Paths.get(config.getString("file")),
    config.getMemorySize("allocate-increment"),
    system)

  private val db = DBMaker
    .appendFileDB(base.toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .fileMmapEnableIfSupported()
    .allocateIncrement(increment.toBytes)
    .make()

  private val work = db.hashMap[String, Array[Byte]]("available")
  private val open = db.hashMap[String, Array[Byte]]("in-progress")
  private val done = db.hashMap[String, Array[Byte]]("finished")


  override def available(): Future[Option[Work]] = transaction {
    work.headOption.map { case (id, w) =>
      work.remove(id)
      open.put(id, w)
      ws.deserialize(w)
    }
  }

  override def done(workId: String, transfer: WorkContentTransfer): Future[Unit] = transaction {
    logger.trace(s"done $workId")
    open.remove(workId)
    done.put(workId, ts.serialize(ConcreteWorkTransfer(transfer)))
  }

  override def release(workId: String): Future[Unit] = transaction {
    logger.trace(s"release $workId")
    Option(open.remove(workId)).foreach(seed => work.put(workId, seed))
  }

  override def releaseAll(ids: Set[String]): Future[Unit] = transaction {
    ids.foreach(release)
  }

  override def addAll(seeds: Set[Work]): Future[Unit] = transaction {
    seeds.foreach(w => work.put(w.id, ws.serialize(w)))
  }

  override def links(workId: String): Future[Option[WorkContentTransfer]] = transaction {
    Option(done.get(workId)).map(ts.deserialize)
  }

  private def transaction[T](f: => T): Future[T] = Future.fromTry {
    val result = Try(f)
    if (result.isSuccess) db.commit() else db.rollback()
    result
  }

  private case object ts extends KryoSerialization[ConcreteWorkTransfer](system)
  private case object ws extends KryoSerialization[Work](system)

}

case class ConcreteWorkTransfer(private val links: Set[ContentLink]) extends WorkContentTransfer {

  override def contents: Stream[ContentLink] = links.toStream

}

object ConcreteWorkTransfer {

  def apply(other: WorkContentTransfer): ConcreteWorkTransfer = other match {
    case c: ConcreteWorkTransfer => c
    case _ => ConcreteWorkTransfer(other.contents.toSet)
  }

}