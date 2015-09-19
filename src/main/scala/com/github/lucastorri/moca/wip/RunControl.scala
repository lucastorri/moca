package com.github.lucastorri.moca.wip

import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

abstract class RunControl(publisher: TaskPublisher) {

  def add(works: Set[Work]): Future[Unit]

  def abort(taskId: String): Future[Unit]

  def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit]

  def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]]

  def links(workId: String): Future[Option[ContentLinksTransfer]]

  final def publish(tasks: Set[Task]): Future[Unit] = publisher.push(tasks)

  def close(): Unit

}

trait TaskPublisher {

  def push(tasks: Set[Task]): Future[Unit]

}
