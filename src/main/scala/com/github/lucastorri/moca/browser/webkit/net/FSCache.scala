package com.github.lucastorri.moca.browser.webkit.net

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.net.{URL, URLConnection}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.security.Permission
import java.util

import com.github.lucastorri.moca.collection.LRUCache
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._

class FSCache(dir: Path, cacheSize: Int) extends Cache with StrictLogging {

  logger.debug(s"Caching files in $dir")
  dir.toFile.mkdirs()

  private[this] val lru = LRUCache[String, FilePair](cacheSize)

  override def apply(conn: URLConnection): URLConnection = synchronized {
    val url = conn.getURL.toString
    val files = FilePair(dir, Url(url).id)
    lru.put(url, files)
    CachedURLConnection(conn, files)
  }

  override def get(url: Url): Option[CacheEntry] =
    Option(lru.get(url.toString)).map(FSCacheEntry)

}

case class FSCacheEntry(files: FilePair) extends CacheEntry {

  override lazy val (status, headers) = {

    val lines = Files.readAllLines(files.headersFile)
    val status = lines.head.split("\\s+")(1).toInt

    val headers = lines.tail
      .map { field => val Array(name, value) = field.split(":", 2); name -> value.trim }
      .groupBy { case (name, value) => name }
      .mapValues(_.map { case (name, value) => value }.toSet)

    status -> headers
  }

  override def content: InputStream =
    new FileInputStream(files.contentFile.toFile)

}

case class FilePair(dir: Path, name: String) {

  def headersFile = dir.resolve(s"$name-headers")
  def contentFile = dir.resolve(s"$name-content")

  def delete(): Unit = {
    headersFile.toFile.delete()
    contentFile.toFile.delete()
  }

}

case class CachedURLConnection(conn: URLConnection, files: FilePair) extends URLConnection(conn.getURL) {

  private var input: InputStream = _

  override def connect(): Unit = {
    conn.connect()

    val header = getHeaderFields
    val status = header.get(null).mkString
    val fields = (header - null).flatMap { case (field, values) => values.map(v => s"$field: $v") }.mkString("\n")
    val serialized = s"$status\n$fields".getBytes(StandardCharsets.UTF_8)
    Files.write(files.headersFile, serialized, StandardOpenOption.CREATE)

    val in = conn.getInputStream
    if (in != null) input = new InterceptedInputStream(in, new FileOutputStream(files.contentFile.toFile))
    else files.contentFile.toFile.createNewFile()
  }

  override def setAllowUserInteraction(allowuserinteraction: Boolean): Unit =
    conn.setAllowUserInteraction(allowuserinteraction)

  override def getIfModifiedSince: Long =
    conn.getIfModifiedSince

  override def getDoInput: Boolean =
    conn.getDoInput

  override def setUseCaches(usecaches: Boolean): Unit =
    conn.setUseCaches(usecaches)

  override def getHeaderFields: util.Map[String, util.List[String]] =
    conn.getHeaderFields

  override def setDoInput(doinput: Boolean): Unit =
    conn.setDoInput(doinput)

  override def getRequestProperty(key: String): String =
    conn.getRequestProperty(key)

  override def getHeaderField(name: String): String =
    conn.getHeaderField(name)

  override def getHeaderField(n: Int): String =
    conn.getHeaderField(n)

  override def getHeaderFieldLong(name: String, Default: Long): Long =
    conn.getHeaderFieldLong(name, Default)

  override def setReadTimeout(timeout: Int): Unit =
    conn.setReadTimeout(timeout)

  override def getExpiration: Long =
    conn.getExpiration

  override def getPermission: Permission =
    conn.getPermission

  override def getContentLengthLong: Long =
    conn.getContentLengthLong

  override def getContent: AnyRef =
    conn.getContent

  override def getContent(classes: Array[Class[_]]): AnyRef =
    conn.getContent(classes)

  override def getURL: URL =
    conn.getURL

  override def getHeaderFieldInt(name: String, Default: Int): Int =
    conn.getHeaderFieldInt(name, Default)

  override def getHeaderFieldDate(name: String, Default: Long): Long =
    conn.getHeaderFieldDate(name, Default)

  override def getHeaderFieldKey(n: Int): String =
    conn.getHeaderFieldKey(n)

  override def addRequestProperty(key: String, value: String): Unit =
    conn.addRequestProperty(key, value)

  override def getLastModified: Long =
    conn.getLastModified

  override def getConnectTimeout: Int =
    conn.getConnectTimeout

  override def setDefaultUseCaches(defaultusecaches: Boolean): Unit =
    conn.setDefaultUseCaches(defaultusecaches)

  override def getOutputStream: OutputStream =
    conn.getOutputStream

  override def setIfModifiedSince(ifmodifiedsince: Long): Unit =
    conn.setIfModifiedSince(ifmodifiedsince)

  override def getReadTimeout: Int =
    conn.getReadTimeout

  override def getDefaultUseCaches: Boolean =
    conn.getDefaultUseCaches

  override def getAllowUserInteraction: Boolean =
    conn.getAllowUserInteraction

  override def setConnectTimeout(timeout: Int): Unit =
    conn.setConnectTimeout(timeout)

  override def getRequestProperties: util.Map[String, util.List[String]] =
    conn.getRequestProperties

  override def getContentLength: Int =
    conn.getContentLength

  override def getDate: Long =
    conn.getDate

  override def getUseCaches: Boolean =
    conn.getUseCaches

  override def getContentType: String =
    conn.getContentType

  override def setRequestProperty(key: String, value: String): Unit =
    conn.setRequestProperty(key, value)

  override def getContentEncoding: String =
    conn.getContentEncoding

  override def getDoOutput: Boolean =
    conn.getDoOutput

  override def setDoOutput(dooutput: Boolean): Unit =
    conn.setDoOutput(dooutput)

  override def getInputStream: InputStream =
    input

  override def toString: String =
    conn.toString

}