package com.github.lucastorri.moca.partition

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

trait PartitionSelector {
  
  def partition(url: Url): String

  final def same(work: Work, url: Url): Boolean =
    same(work.seed, url)

  final def same(url1: Url, url2: Url): Boolean =
    partition(url1) == partition(url2)
  
}
