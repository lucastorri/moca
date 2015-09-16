package com.github.lucastorri.moca.browser.webkit.net

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.net.{HttpURLConnection, Proxy, URL, URLConnection, URLStreamHandler, URLStreamHandlerFactory}
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

  private[this] val lru = LRUCache.withAction[String, FilePair](cacheSize) { (url, files) =>
    files.delete()
  }

  override def apply(conn: URLConnection): URLConnection = synchronized {

    val url = conn.getURL.toString
    val files = FilePair(dir, Url(url).id)
    lru.put(url, files)
    CachedURLConnection(conn.asInstanceOf[HttpURLConnection], files)
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

case class CachedURLConnection(conn: HttpURLConnection, files: FilePair) extends HttpURLConnection(conn.getURL) {

  override def getInputStream: InputStream = {
    var in = conn.getInputStream
    if (in != null) in = new InterceptedInputStream(in, new FileOutputStream(files.contentFile.toFile, true))
    else files.contentFile.toFile.createNewFile()

    val header = getHeaderFields
    if (header != null) {
      val status = header.get(null).mkString
      val fields = (header - null).flatMap { case (field, values) => values.map(v => s"$field: $v") }.mkString("\n")
      val serialized = s"$status\n$fields".getBytes(StandardCharsets.UTF_8)
      Files.write(files.headersFile, serialized, StandardOpenOption.CREATE)
    }

    in
  }

  override def connect(): Unit = conn.connect()

  override def getResponseCode: Int = conn.getResponseCode

  override def setInstanceFollowRedirects(followRedirects: Boolean): Unit = conn.setInstanceFollowRedirects(followRedirects)

  override def setChunkedStreamingMode(chunklen: Int): Unit = conn.setChunkedStreamingMode(chunklen)

  override def getHeaderField(n: Int): String = conn.getHeaderField(n)

  override def getPermission: Permission = conn.getPermission

  override def setRequestMethod(method: String): Unit = conn.setRequestMethod(method)

  override def getHeaderFieldKey(n: Int): String = conn.getHeaderFieldKey(n)

  override def getHeaderFieldDate(name: String, Default: Long): Long = conn.getHeaderFieldDate(name, Default)

  override def setFixedLengthStreamingMode(contentLength: Int): Unit = conn.setFixedLengthStreamingMode(contentLength)

  override def setFixedLengthStreamingMode(contentLength: Long): Unit = conn.setFixedLengthStreamingMode(contentLength)

  override def getRequestMethod: String = conn.getRequestMethod

  override def getResponseMessage: String = conn.getResponseMessage

  override def getInstanceFollowRedirects: Boolean = conn.getInstanceFollowRedirects

  override def getErrorStream: InputStream = conn.getErrorStream

  override def setAllowUserInteraction(allowuserinteraction: Boolean): Unit = conn.setAllowUserInteraction(allowuserinteraction)

  override def getIfModifiedSince: Long = conn.getIfModifiedSince

  override def getDoInput: Boolean = conn.getDoInput

  override def setUseCaches(usecaches: Boolean): Unit = conn.setUseCaches(usecaches)

  override def getHeaderFields: util.Map[String, util.List[String]] = conn.getHeaderFields

  override def getRequestProperty(key: String): String = conn.getRequestProperty(key)

  override def setDoInput(doinput: Boolean): Unit = conn.setDoInput(doinput)

  override def getHeaderField(name: String): String = conn.getHeaderField(name)

  override def getHeaderFieldLong(name: String, Default: Long): Long = conn.getHeaderFieldLong(name, Default)

  override def setReadTimeout(timeout: Int): Unit = conn.setReadTimeout(timeout)

  override def getExpiration: Long = conn.getExpiration

  override def getContentLengthLong: Long = conn.getContentLengthLong

  override def getContent: AnyRef = conn.getContent

  override def getContent(classes: Array[Class[_]]): AnyRef = conn.getContent(classes)

  override def getURL: URL = conn.getURL

  override def getHeaderFieldInt(name: String, Default: Int): Int = conn.getHeaderFieldInt(name, Default)

  override def addRequestProperty(key: String, value: String): Unit = conn.addRequestProperty(key, value)

  override def getLastModified: Long = conn.getLastModified

  override def getConnectTimeout: Int = conn.getConnectTimeout

  override def setDefaultUseCaches(defaultusecaches: Boolean): Unit = conn.setDefaultUseCaches(defaultusecaches)

  override def getOutputStream: OutputStream = conn.getOutputStream

  override def setIfModifiedSince(ifmodifiedsince: Long): Unit = conn.setIfModifiedSince(ifmodifiedsince)

  override def getReadTimeout: Int = conn.getReadTimeout

  override def getDefaultUseCaches: Boolean = conn.getDefaultUseCaches

  override def getAllowUserInteraction: Boolean = conn.getAllowUserInteraction

  override def getRequestProperties: util.Map[String, util.List[String]] = conn.getRequestProperties

  override def setConnectTimeout(timeout: Int): Unit = conn.setConnectTimeout(timeout)

  override def getContentLength: Int = conn.getContentLength

  override def getDate: Long = conn.getDate

  override def getUseCaches: Boolean = conn.getUseCaches

  override def getContentType: String = conn.getContentType

  override def setRequestProperty(key: String, value: String): Unit = conn.setRequestProperty(key, value)

  override def getContentEncoding: String = conn.getContentEncoding

  override def getDoOutput: Boolean = conn.getDoOutput

  override def toString: String = conn.toString

  override def setDoOutput(dooutput: Boolean): Unit = conn.setDoOutput(dooutput)

  override def disconnect(): Unit = conn.disconnect()

  override def usingProxy(): Boolean = conn.usingProxy()
}

class HttpHandler(cache: Cache) extends sun.net.www.protocol.http.Handler with StrictLogging {

  protected override def openConnection(url: URL): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url))
  }

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url, proxy))
  }

}

class HttpsHandler(cache: Cache) extends sun.net.www.protocol.https.Handler with StrictLogging {

  protected override def openConnection(url: URL): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url))
  }

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    cache(super.openConnection(url, proxy))
  }

}

class MocaURLStreamHandlerFactory(cache: Cache) extends URLStreamHandlerFactory {

  override def createURLStreamHandler(protocol: String): URLStreamHandler = protocol match {
    case "http" => new HttpHandler(cache)
    case "https" => new HttpsHandler(cache)
    case _ => null
  }

}