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

  override def serialize(url: Url, content: Content): ByteBuffer = serialize {
    ("url" -> url.toString) ~
    ("status" -> content.status)  ~
    ("headers" -> content.headers) ~
    ("content" -> Base64.getEncoder.encodeToString(content.content.array()))
  }

  override def serialize(url: Url, error: Throwable): ByteBuffer = serialize {
    ("url" -> url.toString) ~
    ("error" -> s"[${error.getClass.getName}] ${error.getMessage}")
  }

  private def serialize(obj: JObject): ByteBuffer =
    charset.encode(compact(render(obj)))

}
