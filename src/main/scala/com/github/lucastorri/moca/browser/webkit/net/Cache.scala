package com.github.lucastorri.moca.browser.webkit.net

import java.io.InputStream
import java.net.URLConnection

import com.github.lucastorri.moca.url.Url

trait Cache {

  def apply(conn: URLConnection): URLConnection

  def get(url: Url): Option[CacheEntry]

}

trait CacheEntry {

  def status: Int

  def headers: Map[String, Set[String]]

  def content: InputStream

}
