package com.github.lucastorri.moca.store.snapshot

import java.nio.file.Paths

import akka.actor.ExtendedActorSystem
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import com.github.lucastorri.moca.store.serialization.KryoSerializer
import com.typesafe.config.Config
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.concurrent.Future

class MapDBSnapshot(config: Config) extends SnapshotStore {

  import context._

  val base = Paths.get(config.getString("directory"))
  val increment = config.getMemorySize("allocate-increment")

  override def preStart(): Unit = {
    base.toFile.getAbsoluteFile.mkdirs()
  }

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    transaction(persistenceId) { db =>
      db.select(criteria)
    }
  }

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    transaction(metadata.persistenceId) { db =>
      db.add(metadata, snapshot)
    }
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    transaction(metadata.persistenceId) { db =>
      db.delete(metadata)
    }
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    transaction(persistenceId) { db =>
      db.deleteAll(criteria)
    }
  }


  private def transaction[T](persistenceId: String)(f: => DBUnit => T): Future[T] = {
    var db: DBUnit = null
    try {
      db = DBUnit(persistenceId)
      val result = f(db)
      db.commit()
      db.close()
      Future.successful(result)
    } catch { case e: Exception =>
      if (db != null) db.roolback()
      Future.failed(e)
    }
  }

  private case class DBUnit(persistenceId: String) extends KryoSerializer[SnapshotContainer](system.asInstanceOf[ExtendedActorSystem]) {

    private val db = DBMaker
      .appendFileDB(base.resolve(persistenceId).toFile)
      .closeOnJvmShutdown()
      .fileMmapEnableIfSupported()
      .allocateIncrement(increment.toBytes)
      .make()

    private val map = db.hashMap[SnapshotMetadata, Array[Byte]]("snapshots")

    def add(meta: SnapshotMetadata, snapshot: Any): Unit =
      map.put(meta, serialize(SnapshotContainer(snapshot)))

    def select(criteria: SnapshotSelectionCriteria): Option[SelectedSnapshot] =
      map.find { case (meta, _) => matches(meta, criteria) }
        .map { case (meta, data) => SelectedSnapshot(meta, deserialize(data).snapshot) }

    def delete(meta: SnapshotMetadata): Unit =
      map.remove(meta)

    def deleteAll(criteria: SnapshotSelectionCriteria): Unit =
      map.keySet().foreach(meta => if (matches(meta, criteria)) map.remove(meta))

    def matches(meta: SnapshotMetadata, criteria: SnapshotSelectionCriteria): Boolean =
      meta != null &&
      meta.sequenceNr >= criteria.minSequenceNr && meta.sequenceNr <= criteria.maxSequenceNr &&
      meta.timestamp >= criteria.minTimestamp && meta.timestamp <= criteria.maxTimestamp

    def commit(): Unit =
      db.commit()

    def roolback(): Unit =
      db.rollback()

    def close(): Unit =
      db.close()

  }

}

case class SnapshotContainer(snapshot: Any)