package com.github.lucastorri.moca.store.control

import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}

case class CombinedLinksTransfer(transfers: Seq[ContentLinksTransfer]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = transfers.toStream.flatMap(_.contents)

}
