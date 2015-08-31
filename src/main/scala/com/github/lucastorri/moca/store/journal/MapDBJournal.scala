package com.github.lucastorri.moca.store.journal

import java.nio.file._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.typesafe.config.Config
import org.mapdb.DBMaker

import scala.collection.immutable
import scala.collection.immutable.NumericRange
import scala.concurrent.Future
import scala.util.Try


class MapDBJournal(config: Config) extends AsyncWriteJournal {

  import context._

  val base = Paths.get(config.getString("file-path"))

  private val _16MB = 16 * 1024 * 1024
  private val db = DBMaker
    .appendFileDB(base.toFile)
    .closeOnJvmShutdown()
    .asyncWriteEnable()
    .cacheLRUEnable()
    .fileMmapEnableIfSupported()
    .allocateIncrement(_16MB)
    .make()

  override def preStart(): Unit = {
    base.toFile.getAbsoluteFile.getParentFile.mkdirs()
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = transaction {
    messages.map { write => Try(DBUnit(write.persistenceId).addAll(write.payload)) }
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = transaction {
    DBUnit(persistenceId).deleteTo(toSequenceNr)
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = transaction {
    DBUnit(persistenceId).last
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] = Future {
    val upperBound = if (toSequenceNr - fromSequenceNr + 1 > max) fromSequenceNr + max - 1 else toSequenceNr
    DBUnit(persistenceId).entries(fromSequenceNr to upperBound).foreach(recoveryCallback)
  }


  private def transaction[T](f: => T): Future[T] = Future {
    val result = f
    db.commit()
    result
  }

  private case class DBUnit(persistenceId: String) {

    val prefix = s"journal-$persistenceId"

    private lazy val map = db.hashMap[Long, PersistentRepr](s"$prefix-entries")
    private lazy val max = db.atomicLong(s"$prefix-max")
    private lazy val min = db.atomicLong(s"$prefix-min")

    def last: Long = max.get()

    def addAll(payload: immutable.Seq[PersistentRepr]): Unit = {
      payload.foreach(entry => map.put(entry.sequenceNr, entry))
      payload.lastOption.foreach(entry => max.set(entry.sequenceNr))
    }

    def deleteTo(lastSequenceNr: Long): Unit = {
      (min.get() to lastSequenceNr).foreach(map.remove)
      min.set(lastSequenceNr)
    }

    def entries(range: NumericRange[Long]): Stream[PersistentRepr] =
      range.toStream.map(map.get)

  }

}
