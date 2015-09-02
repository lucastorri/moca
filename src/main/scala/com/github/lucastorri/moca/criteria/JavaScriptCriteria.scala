package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url
import netscape.javascript.JSObject

import scala.util.Try

trait JavaScriptCriteria extends LinkSelectionCriteria {

  def script: String

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] = {
    val obj = page.exec(script).asInstanceOf[JSObject]
    val length = Try(obj.getMember("length").asInstanceOf[Number].intValue).getOrElse(0)
    val url = page.currentUrl
    (0 until length).flatMap(i => Try(url.resolve(obj.getSlot(i).toString)).toOption).toSet
  }

}

case class StringJSCriteria(script: String) extends JavaScriptCriteria