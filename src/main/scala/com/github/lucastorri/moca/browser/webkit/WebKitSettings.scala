package com.github.lucastorri.moca.browser.webkit

import com.github.lucastorri.moca.browser.BrowserSettings

case class WebKitSettings(
  base: BrowserSettings,
  width: Int,
  height: Int,
  headless: Boolean,
  enableJavaScript: Boolean,
  startingWindows: Int
)
