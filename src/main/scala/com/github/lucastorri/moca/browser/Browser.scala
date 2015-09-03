package com.github.lucastorri.moca.browser

import com.github.lucastorri.moca.url.Url

import scala.concurrent.Future

trait Browser {

  def goTo[T](url: Url)(f: RenderedPage => T): Future[T]

}
