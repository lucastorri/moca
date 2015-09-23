package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.url.Url

case class Work(id: String, seed: Url, criteria: LinkSelectionCriteria)
