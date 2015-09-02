package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case class SameHostCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = {
    val host = link.url.host
    criteria.select(task, link, page).filter(_.host == host)
  }

}
