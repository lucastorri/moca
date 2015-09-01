package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

case class SameHostCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] = {
    val host = link.url.host
    criteria.select(work, link, page).filter(_.host == host)
  }

}
