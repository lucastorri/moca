package com.github.lucastorri.moca.browser

import java.nio.ByteBuffer
import java.security.MessageDigest

case class Content(status: Int, headers: Map[String, Set[String]], content: ByteBuffer, hash: String)

object Content {
  
  def apply(status: Int, header: Map[String, Set[String]], content: ByteBuffer): Content =
    Content(status, header, content, hash(content))

  def hash(content: ByteBuffer): String =
    MessageDigest.getInstance("SHA1")
      .digest(content.array())
      .map("%02x".format(_))
      .mkString

}