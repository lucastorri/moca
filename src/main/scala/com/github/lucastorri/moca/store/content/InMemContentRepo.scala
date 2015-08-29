package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

import scala.collection.mutable
import scala.concurrent.Future

class InMemContentRepo extends ContentRepo {

  private val open = mutable.HashMap.empty[Work, InMemWorkRepo]

  override def apply(work: Work): WorkRepo =
    get(work)

  override def links(work: Work): Set[ContentLink] =
    get(work).links.toSet

  private def get(work: Work): InMemWorkRepo =
    open.getOrElseUpdate(work, new InMemWorkRepo)

}

class InMemWorkRepo extends WorkRepo {

  private[content] val links = mutable.HashSet.empty[ContentLink]

  override def save(url: Url, content: Content): Future[Unit] = {
    links += ContentLink(url, "/no/url/stored")
    Future.successful(())
  }

}
