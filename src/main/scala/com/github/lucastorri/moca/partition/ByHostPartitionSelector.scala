package com.github.lucastorri.moca.partition

import com.github.lucastorri.moca.url.Url

class ByHostPartitionSelector extends PartitionSelector {

  override def apply(url: Url): String =
    url.host

}
