package com.github.lucastorri.moca.browser.webkit.net

import java.io.{FileOutputStream, InputStream, _}
import java.net._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import com.github.lucastorri.moca.collection.LRUCache
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._

case class FSCache(dir: Path, cacheSize: Int) extends Cache with StrictLogging {

  logger.debug(s"Caching files in $dir")
  dir.toFile.mkdirs()

  private[this] val lru = LRUCache[String, FilePair](cacheSize)

  def put(url: URL, files: FilePair): Unit =
    lru.put(url.toString, files)

  override def get(url: Url): Option[CacheEntry] =
    Option(lru.get(url.toString)).map(FSCacheEntry)

  override def  urlStreamHandlerFactory: URLStreamHandlerFactory = new URLStreamHandlerFactory {
    override def createURLStreamHandler(protocol: String): URLStreamHandler = protocol match {
      case "http" => new FSCachedHttpHandler(FSCache.this)
      case "https" => new FSCachedHttpsHandler(FSCache.this)
      case _ => null
    }
  }

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

trait BaseFSCachedHttpsURLConnection { self: HttpURLConnection =>

  def cache: FSCache

  private lazy val files = FilePair(cache.dir, Url(getURL.toString).id)
  
  def saveHeaders(): Unit = {
    cache.put(getURL, files)
    val header = getHeaderFields
    val status = header.get(null).mkString
    val fields = (header - null).flatMap { case (field, values) => values.map(v => s"$field: $v") }.mkString("\n")
    val serialized = s"$status\n$fields".getBytes(StandardCharsets.UTF_8)
    Files.write(files.headersFile, serialized, StandardOpenOption.CREATE)
  }
  
  def saveContent(in: InputStream): InputStream = {
    if (in != null) new InterceptedInputStream(in, new FileOutputStream(files.contentFile.toFile, true))
    else {
      files.contentFile.toFile.createNewFile()
      in
    }
  }

}

class FSCachedHttpHandler(cache: FSCache) extends sun.net.www.protocol.http.Handler with StrictLogging {

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    new FSCachedHttpURLConnection(cache, url, proxy, this)
  }

  class FSCachedHttpURLConnection(val cache: FSCache, url: URL, proxy: Proxy, handler: sun.net.www.protocol.http.Handler)
    extends sun.net.www.protocol.http.HttpURLConnection(url, proxy, handler)
    with BaseFSCachedHttpsURLConnection {

    override def connect(): Unit = {
      super.connect()
      saveHeaders()
    }

    override def getInputStream: InputStream = saveContent(super.getInputStream)
  
  }

}


class FSCachedHttpsHandler(cache: FSCache) extends sun.net.www.protocol.https.Handler with StrictLogging {

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    new FSCachedHttpsURLConnection(cache, url, proxy, this)
  }

  class FSCachedHttpsURLConnection(val cache: FSCache, url: URL, proxy: Proxy, handler: sun.net.www.protocol.https.Handler)
    extends sun.net.www.protocol.https.HttpsURLConnection(url, proxy, handler)
    with BaseFSCachedHttpsURLConnection {

    override def connect(): Unit = {
      super.connect()
      saveHeaders()
    }

    override def getInputStream: InputStream = saveContent(super.getInputStream)

  }

}
