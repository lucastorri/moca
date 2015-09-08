package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

case class SameHostCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = {
    val originalHost = link.url.host
    val renderedHost = page.renderedUrl.host
    criteria.select(task, link, page).filter { l =>
      val host = l.host
      host == originalHost || host == renderedHost
    }
  }

}
