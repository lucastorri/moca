package com.github.lucastorri.moca.store.control

import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.url.Url
import org.scalatest.{MustMatchers, FlatSpec}

class CombinedLinksTransferTest extends FlatSpec with MustMatchers {

  it must "replay all combined transfers" in {

    val transfer = CombinedLinksTransfer(Seq(
      MemLinksContentTransfer(link(1), link(2)),
      MemLinksContentTransfer(link(3), link(4), link(5), link(6)),
      MemLinksContentTransfer(link(7))
    ))

    transfer.contents.map(_.depth) must equal (1 to 7)

  }

  def link(depth: Int) = ContentLink(Url("http://example.com"), "file:///content", depth, "123")

  case class MemLinksContentTransfer(links: ContentLink*) extends ContentLinksTransfer {
    override def contents: Stream[ContentLink] = links.toStream
  }

}
