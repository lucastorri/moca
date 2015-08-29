package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

class InMemWorkRepo extends WorkRepo {

  private val work = mutable.HashSet(
//    "http://www.nokia.com/",
//    "http://www.here.com/",
//    "http://www.example.com/",
    "http://localhost:8000/"
  )
  
  private val open = mutable.HashMap.empty[String, String]

  override def available(): Future[Work] = {
    work.headOption match {
      case Some(seed) =>
        val id = Random.alphanumeric.take(16).mkString
        work.remove(seed)
        open(id) = seed
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
    open.remove(workId).foreach(work.add)
    Future.successful(())
  }

  override def releaseAll(ids: Set[String]): Future[Unit] = {
    ids.foreach(release)
    Future.successful(())
  }

}
