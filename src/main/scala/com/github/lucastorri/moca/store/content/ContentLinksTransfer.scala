package com.github.lucastorri.moca.store.content

trait ContentLinksTransfer { self: Serializable =>

  def contents: Stream[ContentLink]

}
