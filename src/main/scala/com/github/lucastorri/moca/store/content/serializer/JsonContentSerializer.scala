package com.github.lucastorri.moca.store.content.serializer

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64

import com.github.lucastorri.moca.browser.Content
import com.github.lucastorri.moca.url.Url
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class JsonContentSerializer extends ContentSerializer {

  val charset = StandardCharsets.UTF_8

  override def serialize(url: Url, content: Content): ByteBuffer = {
    withUrl(url) { _ ~
      ("status" -> content.status)  ~
      ("headers" -> content.headers) ~
      ("content" -> Base64.getEncoder.encodeToString(content.content.array()))
    }
  }

  override def serialize(url: Url, exception: Throwable): ByteBuffer = {
    withUrl(url) { _ ~
      ("error" -> exception)
    }
  }

  private def withUrl(url: Url)(f: ((String, String)) => JObject): ByteBuffer =
    charset.encode(compact(render(f("url" -> url.toString))))

}
