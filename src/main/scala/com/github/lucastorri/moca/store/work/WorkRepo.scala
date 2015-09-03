package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

trait WorkRepo {

  def addWork(added: Set[Work]): Future[Boolean]

  def links(workId: String): Future[Option[ContentLinksTransfer]]


  def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]]

  def release(taskId: String): Future[Unit]

  def releaseAll(taskIds: Set[String]): Future[Unit]
  
  def addTask(parentTaskId: String, linksDepth: Int, links: Set[Url]): Future[Unit]


  def close(): Unit

}
