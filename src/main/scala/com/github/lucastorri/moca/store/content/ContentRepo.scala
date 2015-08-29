package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

trait ContentRepo {

  def apply(work: Work): WorkContentRepo

  def links(work: Work): Future[Set[ContentLink]]

}

trait WorkContentRepo {

  def save(url: Url, content: Content): Future[Unit]
  
}