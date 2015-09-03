package com.github.lucastorri.moca.browser

import java.nio.charset.Charset

import scala.concurrent.duration.FiniteDuration

case class BrowserSettings(
  charset: Charset,
  loadTimeout: FiniteDuration,
  userAgent: String
)
