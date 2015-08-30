package com.github.lucastorri.moca.browser

import java.nio.charset.Charset

import scala.concurrent.duration.FiniteDuration

case class BrowserSettings(
  width: Int,
  height: Int,
  enableJavaScript: Boolean,
  charset: Charset,
  loadTimeout: FiniteDuration,
  userAgent: String
)
