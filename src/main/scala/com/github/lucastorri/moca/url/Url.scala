package com.github.lucastorri.moca.url

import java.net.{URI, URL}
import java.security.MessageDigest

import crawlercommons.url.EffectiveTldFinder

import scala.util.Try

class Url private[Url](override val toString: String) extends Serializable {

  @transient
  private[this] lazy val _uri = new URI(toString)

  def id: String =
    MessageDigest.getInstance("SHA1")
      .digest(toString.getBytes)
      .map("%02x".format(_))
      .mkString

  def host: String =
    _uri.getHost

  def domain: String =
    EffectiveTldFinder.getAssignedDomain(host)

  def protocol: String =
    _uri.getScheme

  def port: Int =
    _uri.getPort match {
      case -1 if protocol == "http" => 80
      case -1 if protocol == "https" => 443
      case n => n
    }

  def resolve(path: String): Url =
    Url(_uri.resolve(path).toString)

  def root: Url =
    resolve("/")

  override def hashCode: Int =
    toString.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Url => other.toString == toString
    case _ => false
  }

  def toURL: URL =
    _uri.toURL

}

object Url {

  def apply(url: String): Url = { //TODO drop #blah
    new Url(test(url).toURI.normalize().toString)
  }

  def parse(url: String): Option[Url] =
    Try(apply(url)).toOption

  def isValid(url: String): Boolean =
    Try(test(url)).isSuccess

  private def test(url: String): URL =
    new URL(url)

}
