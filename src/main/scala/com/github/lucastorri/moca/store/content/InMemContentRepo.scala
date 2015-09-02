package com.github.lucastorri.moca.store.content
//
//import com.github.lucastorri.moca.browser.Content
//import com.github.lucastorri.moca.role.Work
//import com.github.lucastorri.moca.url.Url
//
//import scala.collection.mutable
//import scala.concurrent.Future
//
//class InMemTaskContentRepo extends TaskContentRepo {
//
//  private[content] val links = mutable.HashSet.empty[ContentLink]
//
//  override def save(url: Url, content: Content): Future[Unit] = {
//    links += ContentLink(url, "/no/url/stored")
//    Future.successful(())
//  }
//
//}
//
//case class InMemContentLinksTransfer(contents: Stream[ContentLink]) extends ContentLinksTransfer
