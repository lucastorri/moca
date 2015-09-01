package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.store.content.{ContentLink, WorkContentTransfer}
import com.github.lucastorri.moca.url.Url

import scala.collection.mutable
import scala.concurrent.Future

class InMemWorkRepo extends WorkRepo {

  private val work = mutable.HashMap.empty[String, String]
  private val open = mutable.HashMap.empty[String, String]
  private val done = mutable.HashMap.empty[String, Set[ContentLink]]

  override def available(): Future[Work] = {
    work.headOption match {
      case Some((id, seed)) =>
        work.remove(id)
        open(id) = seed
        Future.successful(Work(id, Url(seed)))
      case None =>
        Future.failed(NoWorkLeftException)
    }
  }

  override def done(workId: String, transfer: WorkContentTransfer): Future[Unit] = {
    open.remove(workId)
    done.put(workId, transfer.contents.toSet)
    Future.successful(())
  }

  override def release(workId: String): Future[Unit] = {
    open.remove(workId).foreach(seed => work(workId) = seed)
    Future.successful(())
  }

  override def releaseAll(ids: Set[String]): Future[Unit] = {
    ids.foreach(release)
    Future.successful(())
  }

  override def addAll(seeds: Set[Work]): Future[Unit] = {
    seeds.foreach(w => work(w.id) = w.seed.toString)
    Future.successful(())
  }

}
