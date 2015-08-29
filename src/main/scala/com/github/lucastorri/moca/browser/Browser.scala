package com.github.lucastorri.moca.browser

import com.github.lucastorri.moca.url.Url

import scala.concurrent.{ExecutionContext, Future}

trait Browser {

  def goTo[T](url: Url)(f: RenderedPage => T): Future[T]

}

object Browser {

  val defaultSettings = BrowserSettings(1024, 768, enableJavaScript = false)

  def instance()(implicit exec: ExecutionContext): Browser = new Browser {
    override def goTo[T](url: Url)(f: (RenderedPage) => T): Future[T] = {
      BrowserRegion.get().flatMap { region =>
        //TODO timeout result
        val result = region.goTo(url).map(f)
        BrowserRegion.release(region)
        result
      }
    }
  }

}