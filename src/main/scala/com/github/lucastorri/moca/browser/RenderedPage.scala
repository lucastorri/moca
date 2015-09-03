package com.github.lucastorri.moca.browser

import com.github.lucastorri.moca.url.Url

trait RenderedPage {

  def originalUrl: Url

  def currentUrl: Url

  def content: Content

  def html: String

  def exec(javascript: String): AnyRef

  def settings: BrowserSettings

}
