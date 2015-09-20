package com.github.lucastorri.moca.role.worker

import com.github.lucastorri.moca.url.Url

case class Link(url: Url, depth: Int) {

  override def hashCode: Int = url.hashCode

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Link => other.url == url
    case _ => false
  }

}
