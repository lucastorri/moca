package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case class FilteredCriteria(criteria: LinkSelectionCriteria, filter: FilteredCriteria.Filter) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] =
    criteria.select(task, link, page).filter(filter(task, link, page))

}

object FilteredCriteria {

  type Filter = (Task, Link, RenderedPage) => (Url) => Boolean

}
