package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

import scala.concurrent.{ExecutionContext, Future}

object Messages {

  case object Ack
  case object Nack

  case object TaskRequest
  case class TaskOffer(task: Task)
  case class TaskFinished(taskId: String, transfer: ContentLinksTransfer)
  case class IsInProgress(taskId: String)

  case class AddWork(seeds: Set[Work])
  case object TasksAvailable { val topic = "work-announcement" }
  case class AddSubTask(taskId: String, initialDepth: Int, urls: Set[Url])
  case class AbortTask(taskId: String)

  case object ConsistencyCheck

  case class GetLinks(workId: String)
  case class WorkLinks(workId: String, transfer: Option[ContentLinksTransfer])


  implicit class Acked(future: Future[Any]) {
    def acked()(implicit exec: ExecutionContext): Future[Any] = future.filter(_ == Ack)
  }

}
