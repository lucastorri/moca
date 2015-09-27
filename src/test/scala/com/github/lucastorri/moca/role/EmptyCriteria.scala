package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case object EmptyCriteria extends LinkSelectionCriteria {
  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = Set.empty
}