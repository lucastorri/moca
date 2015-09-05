package com.github.lucastorri.moca.browser.webkit.net

import java.io.InputStream
import java.net.URLStreamHandlerFactory

import com.github.lucastorri.moca.url.Url

trait Cache {

  def get(url: Url): Option[CacheEntry]

  def urlStreamHandlerFactory: URLStreamHandlerFactory

}

trait CacheEntry {

  def status: Int

  def headers: Map[String, Set[String]]

  def content: InputStream

}
