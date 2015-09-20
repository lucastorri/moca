package com.github.lucastorri.moca.store.control

import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

trait RunControl {

  def add(works: Set[Work]): Future[Unit]

  def abort(taskId: String): Future[Unit]

  def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit]

  def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]]

  def links(workId: String): Future[Option[ContentLinksTransfer]]

  def close(): Unit


  private var subscriber = Option.empty[Set[Task] => Unit]

  def subscribe(subscriber: Set[Task] => Unit): Unit =
    this.subscriber = Option(subscriber)

  def hasSubscriber: Boolean =
    subscriber.nonEmpty

  def publish(tasks: Set[Task]): Unit = subscriber.foreach(f => f(tasks))

}
