package com.github.lucastorri.moca.browser

import java.nio.ByteBuffer

case class Content(statusCode: Int, headers: Map[String, Set[String]], content: ByteBuffer)
