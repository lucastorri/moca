package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}

case class AllContentLinksTransfer(all: Seq[ContentLink]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = all.toStream

}
