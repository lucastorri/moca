package com.github.lucastorri.moca.browser.webkit.net

import java.net.{Proxy, URL, URLConnection}

import com.typesafe.scalalogging.StrictLogging

class HttpsHandler(cache: Cache) extends sun.net.www.protocol.https.Handler with StrictLogging {

  protected override def openConnection(url: URL): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url))
  }

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url, proxy))
  }

}
