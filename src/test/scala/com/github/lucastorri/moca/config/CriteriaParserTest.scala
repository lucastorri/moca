package com.github.lucastorri.moca.config

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria._
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
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


    parsed.size must equal (3)

    val FilteredCriteria(MaxDepthCriteria(AHrefCriteria, 5), filter) = parsed("criteria-1")
    val MaxDepthCriteria(StringJSCriteria(js), 3) = parsed("criteria-2")

    parsed("criteria-3") must equal (AHrefCriteria)
    filter.getClass must equal(classOf[FakeFilter])
    js must equal (script)
    parsed("unknown") must equal (LinkSelectionCriteria.default)
  }

  it must "allow a default criteria" in {
    val parsed = CriteriaParser.fromString(
      s"""
         |!& default
         |!= max-depth 7
         |!= js test();
       """.stripMargin)

    val MaxDepthCriteria(StringJSCriteria("test();"), 7) = parsed("")
  }

}

class FakeFilter extends FilteredCriteria.Filter {
  override def apply(v1: Task, v2: Link, v3: RenderedPage): (Url) => Boolean = _ => false
}
