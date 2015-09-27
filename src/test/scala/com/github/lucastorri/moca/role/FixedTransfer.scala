package com.github.lucastorri.moca.role

import com.github.lucastorri.moca.store.content.{ContentLinksTransfer, ContentLink}

case class FixedTransfer(items: ContentLink*) extends ContentLinksTransfer {
  override def contents: Stream[ContentLink] = items.toStream
}
