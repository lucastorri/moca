package com.github.lucastorri.moca.store.content.serializer

import java.nio.ByteBuffer

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.url.Url

import scala.util.{Failure, Success, Try}

trait ContentSerializer {

  def serialize(url: Url, content: Content): ByteBuffer

  def serialize(url: Url, exception: Throwable): ByteBuffer

  def serialize(url: Url, result: Try[Content]): ByteBuffer = result match {
    case Success(content) => serialize(url, content)
    case Failure(exception) => serialize(url, exception)
  }

}
