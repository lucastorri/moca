package com.github.lucastorri.moca.store.work

import java.nio.file.Paths

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url
import org.mapdb.DBMaker

import scala.concurrent.Future
import scala.collection.JavaConversions._
import scala.util.Random

class MapDBWorkRepo extends WorkRepo {

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

  private val work = db.hashSet[String]("work-available")
  private val open = db.hashMap[String, String]("work-in-progress")

  if (work.isEmpty) {
    work.add("http://www.here.com/")
    work.add("http://www.example.com/")
  }

  override def available(): Future[Work] = {
    work.headOption match {
      case Some(seed) =>
        val id = Random.alphanumeric.take(16).mkString
        work.remove(seed)
        open.put(id, seed)
        Future.successful(Work(id, Url(seed)))
      case None =>
        Future.failed(NoWorkLeftException)
    }
  }

  override def done(workId: String): Future[Unit] = {
    open.remove(workId)
    Future.successful(())
  }

  override def release(workId: String): Future[Unit] = {
    Option(open.remove(workId)).foreach(work.add)
    Future.successful(())
  }

  override def releaseAll(ids: Set[String]): Future[Unit] = {
    ids.foreach(release)
    Future.successful(())
  }

}
