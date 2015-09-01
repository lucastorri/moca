package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

//TODO create a criteria based on number of urls downloaded and queued (using minion's data structures)

trait LinkSelectionCriteria {

  def select(work: Work, link: OutLink, page: RenderedPage): Set[Url]

}

object LinkSelectionCriteria {

  val default: LinkSelectionCriteria = MaxDepthCriteria(AHrefCriteria, 2)

}