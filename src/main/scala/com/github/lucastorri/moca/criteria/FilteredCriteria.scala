package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

case class FilteredCriteria(criteria: LinkSelectionCriteria, filter: FilteredCriteria.Filter) extends JSoupCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
    criteria.select(work, link, page).filter(filter(work, link, page))

}

object FilteredCriteria {

  type Filter = (Work, OutLink, RenderedPage) => (Url) => Boolean

}