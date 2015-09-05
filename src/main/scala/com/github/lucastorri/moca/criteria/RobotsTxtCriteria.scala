package com.github.lucastorri.moca.criteria

import java.io.InputStream
import java.nio.charset.StandardCharsets

import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.collection.LRUCache
import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRulesParser}
import org.apache.commons.io.IOUtils

import scala.collection.JavaConversions._
import scala.util.Try

case class RobotsTxtCriteria(criteria: LinkSelectionCriteria) extends LinkSelectionCriteria {

  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] =
    criteria.select(task, link, page)
      .filter { url => RobotsTxtCriteria.get(url, page.settings.userAgent).isAllowed(url.toString) }

}

object RobotsTxtCriteria extends StrictLogging {

  val defaultCacheSize = 1024

  private[RobotsTxtCriteria] val cache = LRUCache.ofSize[String, BaseRobotRules](defaultCacheSize)

  private[RobotsTxtCriteria] def get(url: Url, userAgent: String): BaseRobotRules =
    cache.getOrElseUpdate(url.root.toString, fetch(url, userAgent))

  private def fetch(url: Url, userAgent: String): BaseRobotRules = {
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
      content.getBytes(StandardCharsets.UTF_8), "text/plain", userAgent)
  }

}
