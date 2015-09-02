package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case class SameDomainCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = {
    val domain = link.url.domain
    criteria.select(task, link, page).filter(_.domain == domain)
  }

}
