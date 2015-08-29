package com.github.lucastorri.moca.browser

import com.github.lucastorri.moca.url.Url

trait RenderedPage {

  def url: Url

  def content: String

  def links: Set[Url]

}
