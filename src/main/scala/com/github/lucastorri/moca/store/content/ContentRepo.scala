package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

trait ContentRepo {

  def apply(task: Task): TaskContentRepo

  def links(task: Task): ContentLinksTransfer

}

trait TaskContentRepo {

  def save(url: Url, depth: Int, content: Content): Future[Unit]
  
}