package com.github.lucastorri.moca.store.journal

import java.nio.file._

import akka.actor.ActorRef
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.typesafe.config.Config
import org.mapdb.DBMaker

import scala.collection.immutable
import scala.collection.immutable.NumericRange
import scala.concurrent.Future
import scala.util.Try


class MapDBJournal(config: Config) extends AsyncWriteJournal {

  import context._

  val base = Paths.get(config.getString("directory"))
  val increment = config.getMemorySize("allocate-increment")

  override def preStart(): Unit = {
    base.toFile.getAbsoluteFile.mkdirs()
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    Future.sequence {
      messages.map { write =>
        transaction(write.persistenceId) { db =>
          Try(db.addAll(write.payload))
        }
      }
    }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    transaction(persistenceId) { db =>
      db.deleteTo(toSequenceNr)
    }
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    transaction(persistenceId) { db =>
      db.last
    }
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] = {
    transaction(persistenceId, commit = false) { db =>
      val upperBound = if (toSequenceNr - fromSequenceNr + 1 > max) fromSequenceNr + max - 1 else toSequenceNr
      db.entries(fromSequenceNr to upperBound).foreach(recoveryCallback)
    }
  }

  private def transaction[T](persistenceId: String, commit: Boolean = true)(f: DBUnit => T): Future[T] = {
    try {
      val db = DBUnit(persistenceId)
      val result = f(db)
      if (commit) db.commit()
      db.close()
      Future.successful(result)
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  private case class DBUnit(persistenceId: String) extends KryoSerialization[CustomPersistentRepr](system) {

    private lazy val db = DBMaker
      .appendFileDB(base.resolve(persistenceId).toFile)
      .closeOnJvmShutdown()
      .fileMmapEnableIfSupported()
      .allocateIncrement(increment.toBytes)
      .make()

    private lazy val map = db.hashMap[Long, Array[Byte]]("entries")
    private lazy val max = db.atomicLong("max")
    private lazy val min = db.atomicLong("min")

    def last: Long = max.get()

    def addAll(payload: immutable.Seq[PersistentRepr]): Unit = {
      payload.foreach(entry => map.put(entry.sequenceNr, serialize(CustomPersistentRepr(entry))))
      payload.lastOption.foreach(entry => max.set(entry.sequenceNr))
    }

    def deleteTo(lastSequenceNr: Long): Unit = {
      (min.get() to lastSequenceNr).foreach(map.remove)
      min.set(lastSequenceNr)
    }

    def entries(range: NumericRange[Long]): Stream[PersistentRepr] =
      range.toStream.map(map.get).filter(_ != null).map(deserialize)

    def commit(): Unit =
      db.commit()

    def close(): Unit =
      db.close()

  }

}

case class CustomPersistentRepr(
  payload: Any,
  manifest: String,
  sequenceNr: Long,
  persistenceId: String,
  deleted: Boolean,
  sender: ActorRef,
  writerUuid: String
) extends PersistentRepr {

  override def update(sequenceNr: Long, persistenceId: String, deleted: Boolean, sender: ActorRef, writerUuid: String): PersistentRepr =
    copy(sequenceNr = sequenceNr, persistenceId = persistenceId, deleted = deleted, sender = sender, writerUuid = writerUuid)

  override def withPayload(payload: Any): PersistentRepr =
    copy(payload = payload)

  override def withManifest(manifest: String): PersistentRepr =
    copy(manifest = manifest)

}

object CustomPersistentRepr {

  def apply(r: PersistentRepr): CustomPersistentRepr =
    CustomPersistentRepr(r.payload, r.manifest, r.sequenceNr, r.persistenceId, r.deleted, r.sender, r.writerUuid)

}