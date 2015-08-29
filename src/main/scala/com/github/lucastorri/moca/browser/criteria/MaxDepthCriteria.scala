package com.github.lucastorri.moca.browser.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

case class MaxDepthCriteria(criteria: LinkSelectionCriteria, maxDepth: Int) extends LinkSelectionCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
    if (link.depth + 1 > maxDepth) Set.empty
    else criteria.select(work, link, page)

}
