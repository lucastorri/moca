package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url
import org.jsoup.Jsoup

import scala.collection.JavaConversions._

trait JSoupCriteria extends LinkSelectionCriteria {

  def url(page: RenderedPage): Url =
    page.originalUrl

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
     Jsoup.parse(page.html, url(page).toString)
       .select("a")
       .map(a => a.attr("abs:href").trim)
       .flatMap(Url.parse)
       .toSet

 }

object JSoupCriteria extends JSoupCriteria



