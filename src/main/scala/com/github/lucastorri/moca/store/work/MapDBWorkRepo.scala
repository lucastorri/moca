package com.github.lucastorri.moca.store.work

import java.nio.file.Paths
import java.util

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.store.content.{ContentLink, WorkContentTransfer}
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

class MapDBWorkRepo(config: Config) extends WorkRepo with StrictLogging {

  val base = Paths.get(config.getString("file"))
  val increment = config.getMemorySize("allocate-increment")

  private val db = DBMaker
    .appendFileDB(base.toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .fileMmapEnableIfSupported()
    .allocateIncrement(increment.toBytes)
    .make()

  private val work = db.hashMap[String, String]("available")
  private val open = db.hashMap[String, String]("in-progress")
  private val done = db.hashMap[String, java.util.HashSet[ContentLink]]("finished")


  override def available(): Future[Work] = transaction {
    work.headOption match {
      case Some((id, seed)) =>
        work.remove(id)
        open.put(id, seed)
        Work(id, Url(seed))
      case None =>
        throw NoWorkLeftException
    }
  }

  override def done(workId: String, transfer: WorkContentTransfer): Future[Unit] = transaction {
    logger.trace(s"done $workId")
    open.remove(workId)
    done.put(workId, new util.HashSet[ContentLink](transfer.contents))
  }

  override def release(workId: String): Future[Unit] = transaction {
    logger.trace(s"release $workId")
    Option(open.remove(workId)).foreach(seed => work.put(workId, seed))
  }

  override def releaseAll(ids: Set[String]): Future[Unit] = transaction {
    ids.foreach(release)
  }

  override def addAll(seeds: Set[Work]): Future[Unit] = transaction {
    seeds.foreach(w => work.put(w.id, w.seed.toString))
  }

  private def transaction[T](f: => T): Future[T] = Future.fromTry {
    val result = Try(f)
    if (result.isSuccess) db.commit() else db.rollback()
    result
  }

}
