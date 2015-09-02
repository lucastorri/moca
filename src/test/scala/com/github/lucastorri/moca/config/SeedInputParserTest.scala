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
         |http://www.example.com|1|custom-criteria
         |http://www.w3c.com|2
         |http://www.iana.org/||custom-criteria
         |https://www.wikipedia.org/
       """.stripMargin

    val work = SeedInputParser.fromString(input)
    val Work(id1, url1, criteria1) = work(0)
    val Work(id2, url2, criteria2) = work(1)
    val Work(id3, url3, criteria3) = work(2)
    val Work(id4, url4, criteria4) = work(3)

    work.size must equal (4)

    id1 must equal ("1")
    id2 must equal ("2")
    id3 must equal (url3.id)
    id4 must equal (url4.id)

    url1 must equal (Url("http://www.example.com"))
    url2 must equal (Url("http://www.w3c.com"))
    url3 must equal (Url("http://www.iana.org/"))
    url4 must equal (Url("https://www.wikipedia.org/"))

    criteria1 must equal (CriteriaParser.fromString(input)("custom-criteria"))
    criteria2 must equal (LinkSelectionCriteria.default)
    criteria3 must equal (CriteriaParser.fromString(input)("custom-criteria"))
    criteria4 must equal (LinkSelectionCriteria.default)
  }

}
