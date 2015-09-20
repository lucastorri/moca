package com.github.lucastorri.moca.browser.html

import java.io.ByteArrayInputStream
import javax.xml.parsers.DocumentBuilderFactory

import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.scalatest.{MustMatchers, FlatSpec}
import org.w3c.dom.Document
import scala.collection.JavaConversions._

class W3CDocumentSerializerTest extends FlatSpec with MustMatchers {

  val invalidHtml = """
               |<html>
               |  <head/>
               |  <body>
               |    <div id="box">
               |      <a class="addthis_button_facebook_like" fb:like:layout="button_count"></a>
               |    </div>
               |  </body>
               |</html>""".stripMargin


  it must "complain about invalid namespaces on attributes using StrictParser" in {

    an[Exception] must be thrownBy {
      W3CDocumentSerializer.StrictParser.toString(doc(invalidHtml))
    }

  }

  it must "not complain about unknown namespaces using RelaxedParser" in {

    val serialized = W3CDocumentSerializer.RelaxedParser.toString(doc(invalidHtml))

    val div = Jsoup.parse(serialized).body().child(0)
    val a = div.child(0)

    div.tagName must equal ("div")
    attributes(div) must equal (Map(
      "id" -> "box"))

    a.tagName must equal ("a")
    attributes(a) must equal (Map(
      "like:layout" -> "button_count",
      "class" -> "addthis_button_facebook_like"))

  }

  def attributes(e: Element): Map[String, String] = {
    e.attributes().map(attr => attr.getKey -> attr.getValue).toMap
  }

  def doc(html: String): Document = {
    val factory = DocumentBuilderFactory.newInstance()
    factory.setNamespaceAware(false)
    val builder = factory.newDocumentBuilder()
    builder.parse(new ByteArrayInputStream(html.getBytes))
  }

}
