package com.github.lucastorri.moca.role.worker

import com.github.lucastorri.moca.role.worker.Minion.Event.Found
import com.github.lucastorri.moca.url.Url

case class OutLink(url: Url, depth: Int) {

  override def hashCode: Int = url.hashCode

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: OutLink => other.url == url
    case _ => false
  }

}

object OutLink {

  def all(found: Found): Set[OutLink] =
    found.urls.map(url => OutLink(url, found.depth))

}