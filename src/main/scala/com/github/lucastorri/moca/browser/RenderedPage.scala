package com.github.lucastorri.moca.browser

import com.github.lucastorri.moca.url.Url

trait RenderedPage {

  def originalUrl: Url

  def renderedUrl: Url

  def renderedHtml: String

  def renderedContent: Content

  def originalContent: Content

  def exec(javascript: String): AnyRef

  def settings: BrowserSettings

}
