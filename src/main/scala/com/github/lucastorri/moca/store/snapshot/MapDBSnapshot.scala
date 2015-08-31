package com.github.lucastorri.moca.store.snapshot

import java.nio.file.Paths

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.typesafe.config.Config
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.concurrent.Future

class MapDBSnapshot(config: Config) extends SnapshotStore {

  import context._

  val base = Paths.get(config.getString("file-path"))

  private val _64MB = 64 * 1024 * 1024
  private val db = DBMaker
    .appendFileDB(base.toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .fileMmapEnableIfSupported()
    .allocateIncrement(_64MB)
    .make()

  override def preStart(): Unit = {
    base.toFile.getAbsoluteFile.getParentFile.mkdirs()
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = transaction {
    DBUnit(persistenceId).select(criteria)
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = transaction {
    DBUnit(metadata.persistenceId).add(metadata, snapshot)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = transaction {
    DBUnit(metadata.persistenceId).delete(metadata)
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = transaction {
    DBUnit(persistenceId).deleteAll(criteria)
  }


  private def transaction[T](f: => T): Future[T] = Future {
    val result = f
    db.commit()
    result
  }

  private case class DBUnit(persistenceId: String) extends KryoSerialization[Any](system) {

    private val map = db.hashMap[SnapshotMetadata, Array[Byte]](s"snapshots-$persistenceId")

    def add(meta: SnapshotMetadata, snapshot: Any): Unit =
      map.put(meta, serialize(snapshot))

    def select(criteria: SnapshotSelectionCriteria): Option[SelectedSnapshot] =
      map.find { case (meta, _) => matches(meta, criteria) }
        .map { case (meta, snapshot) => SelectedSnapshot(meta, deserialize(snapshot)) }

    def delete(meta: SnapshotMetadata): Unit =
      map.remove(meta)

    def deleteAll(criteria: SnapshotSelectionCriteria): Unit =
      map.keySet().foreach(meta => if (matches(meta, criteria)) map.remove(meta))

    def matches(meta: SnapshotMetadata, criteria: SnapshotSelectionCriteria): Boolean =
      meta != null &&
      meta.sequenceNr >= criteria.minSequenceNr && meta.sequenceNr <= criteria.maxSequenceNr &&
      meta.timestamp >= criteria.minTimestamp && meta.timestamp <= criteria.maxTimestamp

  }

}
