package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case class MaxDepthCriteria(criteria: LinkSelectionCriteria, maxDepth: Int) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] =
    if (link.depth >= maxDepth) Set.empty
    else criteria.select(task, link, page)

}
