package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url
import org.jsoup.Jsoup

import scala.collection.JavaConversions._

case object AHrefCriteria extends LinkSelectionCriteria {

  def url(page: RenderedPage): Url =
    page.renderedUrl

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] =
    Jsoup.parse(page.renderedHtml, url(page).toString)
      .select("a")
      .map(a => a.attr("abs:href").trim)
      .flatMap(Url.parse)
      .toSet

}
