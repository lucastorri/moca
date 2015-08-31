package com.github.lucastorri.moca.store.content

trait WorkContentTransfer {

  def contents: Stream[ContentLink]

}
