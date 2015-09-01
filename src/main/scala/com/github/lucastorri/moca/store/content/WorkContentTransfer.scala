package com.github.lucastorri.moca.store.content

trait WorkContentTransfer { self: Serializable =>

  def contents: Stream[ContentLink]

}
