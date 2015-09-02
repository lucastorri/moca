package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

import scala.concurrent.duration._

case class Task(id: String, seeds: Set[Url], criteria: LinkSelectionCriteria, initialDepth: Int, partition: String) {

  def intervalBetweenRequests: FiniteDuration = 5.seconds

  def select(link: Link, page: RenderedPage): Set[Url] =
    criteria.select(this, link, page)

}
