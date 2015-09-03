package com.github.lucastorri.moca.browser.webkit

import com.github.lucastorri.moca.async._
import com.github.lucastorri.moca.browser.{Browser, BrowserProvider, BrowserSettings, RenderedPage}
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

class WebKitBrowserProvider(config: Config, exec: ExecutionContext, settings: BrowserSettings) extends BrowserProvider {

  private implicit val ctx = exec

  WebKitBrowserProvider.set {
    WebKitSettings(settings,
      config.getInt("width"),
      config.getInt("height"),
      config.getBoolean("headless"),
      config.getBoolean("enable-js"))
  }

  override def instance(): Browser = new Browser with StrictLogging {
    override def goTo[T](url: Url)(f: (RenderedPage) => T): Future[T] = {
      BrowserWindow.get().flatMap { region =>
        logger.trace(s"Got region ${region.id}")
        val result = timeout(settings.loadTimeout)(region.goTo(url).map(f))
        result.onComplete(_ => BrowserWindow.release(region))
        result
      }
    }
  }

}

object WebKitBrowserProvider {

  private[this] var _settings: WebKitSettings = null

  private[webkit] def set(settings: WebKitSettings): Unit = {
    if (_settings != null) sys.error(s"Can't instantiate ${classOf[WebKitBrowserProvider].getSimpleName} more than once")
    _settings = settings
  }

  private[webkit] def settings: WebKitSettings =
    _settings

}