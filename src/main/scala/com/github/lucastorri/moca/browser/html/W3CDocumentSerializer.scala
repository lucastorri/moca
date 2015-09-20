package com.github.lucastorri.moca.browser.html

import java.io.StringWriter
import javax.xml.stream.XMLOutputFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stax.StAXResult
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{TransformerException, ErrorListener, OutputKeys, TransformerFactory}

import org.jdom2.UncheckedJDOMFactory
import org.w3c.dom.Document
import org.w3c.dom.html.HTMLDocument
import org.jdom2.input.DOMBuilder
import org.jdom2.output.XMLOutputter

object W3CDocumentSerializer {

  trait Parser {

    def toString(doc: Document): String

  }

  case object StrictParser extends Parser{

    override def toString(doc: Document): String = {
      val transformer = TransformerFactory.newInstance().newTransformer()
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
      transformer.setOutputProperty(OutputKeys.METHOD, "html")
      val writer = new StringWriter()
      transformer.transform(new DOMSource(doc), new StreamResult(writer))
      writer.flush()
      writer.toString
    }

  }

  case object RelaxedParser extends Parser {

    override def toString(doc: Document): String = {
      val builder = new DOMBuilder()
      builder.setFactory(new UncheckedJDOMFactory)
      val jdomDoc = builder.build(doc)
      new XMLOutputter().outputString(jdomDoc)
    }

  }

  def toString(doc: Document, parser: Parser = RelaxedParser): String = {
    parser.toString(doc)
  }

}
