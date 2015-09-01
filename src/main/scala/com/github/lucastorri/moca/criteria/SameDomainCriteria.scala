package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

case class SameDomainCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] = {
    val domain = link.url.domain
    criteria.select(work, link, page).filter(_.domain == domain)
  }

}
