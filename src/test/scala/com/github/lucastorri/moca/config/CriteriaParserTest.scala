package com.github.lucastorri.moca.config

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria._
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url
import org.scalatest.{FlatSpec, MustMatchers}

class CriteriaParserTest extends FlatSpec with MustMatchers {

  it must "construct a criteria from lines" in {

    val script = "console.log('hello world!');"

    val parsed = CriteriaParser.fromString(s"""
        |!& criteria-1
        |!= filter com.github.lucastorri.moca.config.FakeFilter
        |!= max-depth 5
        |!= a-href
        |!& criteria-2
        |!= max-depth 3
        |!= js $script
        |
        |!& criteria-3
        |!= a-href
      """.stripMargin)


    parsed.named.size must equal (3)

    val FilteredCriteria(MaxDepthCriteria(AHrefCriteria, 5), filter) = parsed.named("criteria-1")
    val MaxDepthCriteria(StringJSCriteria(js), 3) = parsed.named("criteria-2")

    parsed.named("criteria-3") must equal (AHrefCriteria)
    filter.getClass must equal(classOf[FakeFilter])
    js must equal (script)
    parsed.named("unknown") must equal (LinkSelectionCriteria.default)
  }

}

class FakeFilter extends FilteredCriteria.Filter {
  override def apply(v1: Work, v2: OutLink, v3: RenderedPage): (Url) => Boolean = _ => false
}
