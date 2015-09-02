package com.github.lucastorri.moca.criteria.filter

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.FilteredCriteria
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url

class SocialMediaFilter extends FilteredCriteria.Filter {

  override def apply(t: Task, l: Link, p: RenderedPage): (Url) => Boolean =
    url => !SocialMediaFilter.blacklistedDomains.contains(url.domain)

}

object SocialMediaFilter {

  val blacklistedDomains = Set(
    "facebook.com",
    "twitter.com"
  )

}