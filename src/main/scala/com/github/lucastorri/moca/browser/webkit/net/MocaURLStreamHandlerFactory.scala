package com.github.lucastorri.moca.browser.webkit.net

import java.net.{URLStreamHandler, URLStreamHandlerFactory}

class MocaURLStreamHandlerFactory(cache: Cache) extends URLStreamHandlerFactory {

  override def createURLStreamHandler(protocol: String): URLStreamHandler = protocol match {
    case "http" => new HttpHandler(cache)
    case "https" => new HttpsHandler(cache)
    case _ => null
  }

}
