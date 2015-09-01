package com.github.lucastorri.moca.store.content

trait WorkContentTransfer extends Serializable {

  def contents: Stream[ContentLink]

}
