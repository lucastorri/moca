package com.github.lucastorri.moca.criteria

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.Map.Entry

import com.github.lucastorri.moca.browser.{Browser, RenderedPage}
import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.role.worker.OutLink
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRulesParser}
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._

import scala.util.Try

case class RobotsTxtCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(work: Work, link: OutLink, page: RenderedPage): Set[Url] =
    criteria.select(work, link, page)
      .filter { url => RobotsTxtCriteria.get(url).isAllowed(url.toString) }

}

object RobotsTxtCriteria extends StrictLogging {

  val defaultCacheSize = 1024

  private[RobotsTxtCriteria] val cache = LRUCache[String, BaseRobotRules](1024)

  private[RobotsTxtCriteria] def get(url: Url): BaseRobotRules =
    cache.getOrElseUpdate(url.root.toString, fetch(url))

  private def fetch(url: Url): BaseRobotRules = {
    val robots = url.resolve("/robots.txt").toURL
    var in: InputStream = null
    val content =
      try {
        in = robots.openStream()
        IOUtils.toString(in)
      } catch {
        case e: Exception =>
          logger.error(s"Could not fetch robots.txt at $robots")
          ""
      } finally {
        if (in != null) Try(in.close())
      }

    new SimpleRobotRulesParser().parseContent(robots.toString,
      content.getBytes(StandardCharsets.UTF_8), "text/plain", Browser.defaultSettings.userAgent)
  }

  case class LRUCache[K, V](maxSize: Int) extends java.util.LinkedHashMap[K, V](16, 0.75f, true) {
    override def removeEldestEntry(eldest: Entry[K, V]): Boolean = size > maxSize
  }

}
