package com.github.lucastorri.moca.config

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url
import org.scalatest.{FlatSpec, MustMatchers}

class SeedInputParserTest extends FlatSpec with MustMatchers {

  it must "parse an input file" in {

    val input = s"""
         !& custom-criteria
         |!= a-href
         |
         |1|http://www.example.com|custom-criteria
         |2|http://www.w3c.com
       """.stripMargin

    val work = SeedInputParser.fromString(input)
    val Work(id1, url1, criteria1) = work.head
    val Work(id2, url2, criteria2) = work.last

    work.size must equal (2)

    id1 must equal ("1")
    id2 must equal ("2")

    url1 must equal (Url("http://www.example.com"))
    url2 must equal (Url("http://www.w3c.com"))

    criteria1 must equal (CriteriaParser.fromString(input)("custom-criteria"))
    criteria2 must equal (LinkSelectionCriteria.default)
  }

}
