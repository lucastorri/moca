package com.github.lucastorri.moca.browser

import java.nio.charset.StandardCharsets

import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import com.github.lucastorri.moca.async.timeout

trait Browser {

  def goTo[T](url: Url)(f: RenderedPage => T): Future[T]

}

object Browser extends StrictLogging {

  val defaultSettings = BrowserSettings(
    1024, 768,
    enableJavaScript = false,
    StandardCharsets.UTF_8,
    15.seconds,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36")

  def instance()(implicit exec: ExecutionContext): Browser = new Browser {
    override def goTo[T](url: Url)(f: (RenderedPage) => T): Future[T] = {
      BrowserRegion.get().flatMap { region =>
        logger.trace(s"Got region ${region.id}")
        val result = timeout(defaultSettings.loadTimeout)(region.goTo(url).map(f))
        result.onComplete(_ => BrowserRegion.release(region))
        result
      }
    }
  }

}