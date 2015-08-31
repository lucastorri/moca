package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Seed

import scala.concurrent.Future

trait WorkRepo {

  def available(): Future[Work]

  def done(workId: String): Future[Unit]

  def release(workId: String): Future[Unit]

  def releaseAll(workIds: Set[String]): Future[Unit]

  def addAll(seeds: Set[Seed]): Future[Unit]

}

case object NoWorkLeftException extends Exception