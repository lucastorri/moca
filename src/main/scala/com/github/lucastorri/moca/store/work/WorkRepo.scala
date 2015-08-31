package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work

import scala.concurrent.Future

trait WorkRepo {

  def available(): Future[Work]

  def done(workId: String): Future[Unit]

  def release(workId: String): Future[Unit]

  def releaseAll(workIds: Set[String]): Future[Unit]

  def addAll(seeds: Set[Work]): Future[Unit]

}

case object NoWorkLeftException extends Exception