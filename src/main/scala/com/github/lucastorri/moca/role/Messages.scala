package com.github.lucastorri.moca.role

object Messages {

  case object WorkRequest
  case class WorkOffer(work: Work)
  case object Ack
  case class WorkDone(workId: String)

}
