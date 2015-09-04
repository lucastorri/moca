package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait ContentRepo {

  def apply(task: Task): TaskContentRepo

  def links(task: Task): ContentLinksTransfer

}

trait TaskContentRepo {

  def save(url: Url, depth: Int, content: Content): Future[Unit] =
    save(url, depth, Success(content))

  def save(url: Url, depth: Int, error: Throwable): Future[Unit] =
    save(url, depth, Failure(error))

  def save(url: Url, depth: Int, content: Try[Content]): Future[Unit]

}