package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.store.content.ContentLink

object Messages {

  case object WorkRequest
  case class WorkOffer(work: Work)
  case object Ack
  case class WorkDone(workId: String, contentLinks: Set[ContentLink])

}
