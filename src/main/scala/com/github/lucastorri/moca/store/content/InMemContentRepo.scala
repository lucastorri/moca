package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

import scala.collection.mutable
import scala.concurrent.Future

class InMemContentRepo extends ContentRepo {

  private val open = mutable.HashMap.empty[Work, InMemWorkContentRepo]

  override def apply(work: Work): WorkContentRepo =
    get(work)

  override def links(work: Work): Set[ContentLink] =
    get(work).links.toSet

  private def get(work: Work): InMemWorkContentRepo =
    open.getOrElseUpdate(work, new InMemWorkContentRepo)

}

class InMemWorkContentRepo extends WorkContentRepo {

  private[content] val links = mutable.HashSet.empty[ContentLink]

  override def save(url: Url, content: Content): Future[Unit] = {
    links += ContentLink(url, "/no/url/stored")
    Future.successful(())
  }

}
