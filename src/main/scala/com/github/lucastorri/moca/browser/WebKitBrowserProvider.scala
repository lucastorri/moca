package com.github.lucastorri.moca.browser

import scala.concurrent.ExecutionContext

class WebKitBrowserProvider extends BrowserProvider {

  override def instance()(implicit exec: ExecutionContext): Browser =
    Browser.instance()

}
