package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.store.content.ContentLink

import scala.concurrent.{ExecutionContext, Future}

object Messages {

  case object WorkRequest
  case class WorkOffer(work: Work)
  case object Ack
  case class WorkDone(workId: String, contentLinks: Set[ContentLink])
  case class InProgress(workId: String)

  implicit class Acked(future: Future[Any]) {

    def acked()(implicit exec: ExecutionContext): Future[Any] = future.filter(_ == Ack)

  }

}
