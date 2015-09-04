package com.github.lucastorri.moca.browser.webkit.net

import java.io.{ByteArrayInputStream, InputStream}

case object EmptyCacheEntry extends CacheEntry {

  override def status: Int = -1

  override def content: InputStream = new ByteArrayInputStream(Array.empty)

  override def headers: Map[String, Set[String]] = Map.empty

}
