package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.store.content.WorkContentTransfer

import scala.concurrent.{ExecutionContext, Future}

object Messages {

  case object Ack

  case object WorkRequest
  case class WorkOffer(work: Work)
  case class WorkFinished(workId: String, transfer: WorkContentTransfer)
  case class InProgress(workId: String)

  case class AddSeeds(seeds: Set[Work])
  case object WorkAvailable { val topic = "work-announcement" }

  case object ConsistencyCheck

  implicit class Acked(future: Future[Any]) {

    def acked()(implicit exec: ExecutionContext): Future[Any] = future.filter(_ == Ack)

  }

}
