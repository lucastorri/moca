package com.github.lucastorri.moca.browser

import scala.concurrent.ExecutionContext

trait BrowserProvider {

  def instance()(implicit exec: ExecutionContext): Browser

}
