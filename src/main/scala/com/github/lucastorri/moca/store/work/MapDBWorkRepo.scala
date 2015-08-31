package com.github.lucastorri.moca.store.work

import java.nio.file.Paths

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DBMaker

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.Try

class MapDBWorkRepo extends WorkRepo with StrictLogging {

  //TODO make configurable
  private val path = Paths.get("works")

  private val _1MB = 1 * 1024 * 1024
  private val db = DBMaker
    .appendFileDB(path.toFile)
    .closeOnJvmShutdown()
    .cacheLRUEnable()
    .fileMmapEnableIfSupported()
    .allocateIncrement(_1MB)
    .make()

  private val work = db.hashMap[String, String]("work-available")
  private val open = db.hashMap[String, String]("work-in-progress")


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

  override def done(workId: String): Future[Unit] = transaction {
    logger.trace(s"done $workId")
    open.remove(workId)
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
