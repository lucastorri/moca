package com.github.lucastorri.moca.url

import java.net.URL

import scala.util.Try

class Url private[Url](override val toString: String) extends Serializable {

  override def hashCode: Int = toString.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: Url => other.toString == toString
    case _ => false
  }

}

object Url {

  def apply(url: String): Url = {
    test(url)
    new Url(url)
  }

  def parse(url: String): Option[Url] =
    Try(apply(url)).toOption

  def isValid(url: String): Boolean =
    Try(test(url)).isSuccess

  private def test(url: String): Unit =
    new URL(url)

}
