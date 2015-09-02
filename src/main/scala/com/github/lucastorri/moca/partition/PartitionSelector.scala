package com.github.lucastorri.moca.partition

import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.url.Url

trait PartitionSelector {
  
  def apply(url: Url): String

  final def same(task: Task, url: Url): Boolean =
    task.partition == apply(url)

  final def same(url1: Url, url2: Url): Boolean =
    apply(url1) == apply(url2)
  
}
