package com.github.lucastorri.moca.store.content.serializer

import java.nio.ByteBuffer

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.url.Url

trait ContentSerializer {

  def serialize(url: Url, content: Content): ByteBuffer

  def serialize(url: Url, exception: Throwable): ByteBuffer

}
