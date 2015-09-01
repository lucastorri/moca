package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

import scala.concurrent.duration._

case class Work(id: String, seed: Url) {

  def criteria: LinkSelectionCriteria = LinkSelectionCriteria.default

  def intervalBetweenRequests: FiniteDuration = 5.seconds

  def select(link: OutLink, page: RenderedPage): Set[Url] =
    criteria.select(this, link, page)

}
