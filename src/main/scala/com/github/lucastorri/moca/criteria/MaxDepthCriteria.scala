package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

case class MaxDepthCriteria(criteria: LinkSelectionCriteria, maxDepth: Int) extends LinkSelectionCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
    if (link.depth >= maxDepth) Set.empty
    else criteria.select(work, link, page)

}
