package com.github.lucastorri.moca.criteria

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

//TODO create a criteria with support for robots.txt

trait LinkSelectionCriteria {

  def select(work: Work, link: OutLink, page: RenderedPage): Set[Url]

}






