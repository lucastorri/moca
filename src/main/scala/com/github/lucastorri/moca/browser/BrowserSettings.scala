package com.github.lucastorri.moca.browser

import java.nio.charset.Charset

case class BrowserSettings(
  width: Int,
  height: Int,
  enableJavaScript: Boolean,
  charset: Charset,
  userAgent: String
)
