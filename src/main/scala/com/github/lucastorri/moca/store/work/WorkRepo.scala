package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.store.content.WorkContentTransfer

import scala.concurrent.Future

trait WorkRepo {

  def available(): Future[Option[Work]]

  def done(workId: String, transfer: WorkContentTransfer): Future[Unit]

  def release(workId: String): Future[Unit]

  def releaseAll(workIds: Set[String]): Future[Unit]

  def addAll(seeds: Set[Work]): Future[Unit]

  def links(workId: String): Future[Option[WorkContentTransfer]]

}
