package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

trait LinkSelectionCriteria {

  def select(task: Task, link: Link, page: RenderedPage): Set[Url]

}

object LinkSelectionCriteria {

  val default: LinkSelectionCriteria = MaxDepthCriteria(AHrefCriteria, 2)

}