package com.github.lucastorri.moca.url

import java.net.{URL => JavaURL}
import java.security.MessageDigest

import crawlercommons.url.EffectiveTldFinder
import io.mola.galimatias.{URL => NormalizedURL}

import scala.util.Try

class Url private[Url](override val toString: String) extends Serializable {

  @transient
  private[this] lazy val _url = NormalizedURL.parse(toString)

  def id: String =
    MessageDigest.getInstance("SHA1")
      .digest(toString.getBytes)
      .map("%02x".format(_))
      .mkString

  def host: String =
    _url.host().toString

  def domain: String =
    EffectiveTldFinder.getAssignedDomain(host)

  def protocol: String =
    _url.scheme()

  def port: Int =
    _url.port()

  def resolve(path: String): Url =
    try Url(_url.resolve(path).toString)
    catch { case e: Exception => throw InvalidPathResolutionException(toString, path, e) }

  def resolveOption(path: String): Option[Url] =
    Try(resolve(path)).toOption

  def root: Url =
    resolve("/")

  override def hashCode: Int =
    toString.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Url => other.toString == toString
    case _ => false
  }

  def toURL: JavaURL =
    _url.toJavaURL

}

object Url {

  def apply(url: String): Url =
    new Url(normalizeAndTest(url).toString)

  def parse(url: String): Option[Url] =
    Try(apply(url)).toOption

  def isValid(url: String): Boolean =
    Try(normalizeAndTest(url)).isSuccess

  private def normalizeAndTest(url: String): NormalizedURL = {
    try {
      val cleaned = url.indexOf('#') match {
        case -1 => url
        case n => url.substring(0, n)
      }
      val normalized = NormalizedURL.parse(cleaned)
      normalized.scheme() match {
        case "http" | "https" => normalized
        case _ => throw InvalidUrlException(normalized.toString, null)
      }
    } catch {
      case e: InvalidUrlException => throw e
      case e: Exception => throw InvalidUrlException(url, e)
    }
  }

}

case class InvalidUrlException(url: String, e: Exception) extends Exception(s"Invalid url '$url'", e)

case class InvalidPathResolutionException(url: String, path: String, e: Exception) extends Exception(s"Invalid path '$path' to resolve against '$url'", e)
