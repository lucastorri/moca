package com.github.lucastorri.moca.criteria.filter

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.FilteredCriteria
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url

class SocialMediaFilter extends FilteredCriteria.Filter {

  override def apply(w: Work, l: OutLink, p: RenderedPage): (Url) => Boolean =
    url => !SocialMediaFilter.blacklistedDomains.contains(url.domain)

}

object SocialMediaFilter {

  val blacklistedDomains = Set(
    "facebook.com",
    "twitter.com"
  )

}