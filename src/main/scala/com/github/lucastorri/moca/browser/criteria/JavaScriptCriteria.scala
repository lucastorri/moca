package com.github.lucastorri.moca.browser.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

trait JavaScriptCriteria extends LinkSelectionCriteria {

  def script: String

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
    page.exec(script).asInstanceOf[Array[String]].map(Url.apply).toSet

}

case class StringJSCriteria(script: String) extends JavaScriptCriteria