package com.github.lucastorri.moca.config

import com.github.lucastorri.moca.criteria._

import scala.collection.mutable


object CriteriaParser {

  val factories = Map[String, (IndexedSeq[String], LinkSelectionCriteria) => LinkSelectionCriteria](
    "a-href" -> { (_, _) => AHrefCriteria },
    "js" -> { (params, _) => StringJSCriteria(params.head) },
    "max-depth" -> { (params, current) => MaxDepthCriteria(current, params.head.toInt) },
    "filter" -> { (params, current) => FilteredCriteria(current, filter(params.head)) },
    "same-host" -> { (_, current) => SameHostCriteria(current) },
    "same-domain" -> { (_, current) => SameDomainCriteria(current) }
  )

  private[CriteriaParser] def filter(className: String): FilteredCriteria.Filter =
    Class.forName(className).newInstance().asInstanceOf[FilteredCriteria.Filter]

  def fromString(str: String): Map[String, LinkSelectionCriteria] =
    fromLines(str.lines)

  def fromLines(input: Iterable[String]): Map[String, LinkSelectionCriteria] =
    fromLines(input.iterator)

  def fromLines(input: Iterator[String]): Map[String, LinkSelectionCriteria] = {
    val map = grouped(input).map(parse).toMap
    map.withDefaultValue(map.getOrElse("default", LinkSelectionCriteria.default))
  }

  private def grouped(input: Iterator[String]): Seq[Seq[String]] = {
    val groups = mutable.ListBuffer.empty[Seq[String]]
    var current = Seq.empty[String]

    def add() = if (current.nonEmpty) {
      groups += current
      current = Seq.empty
    }

    while (input.hasNext) {
      val line = input.next().trim
      line.take(2).mkString match {
        case "!&" if current.isEmpty => current :+= line
        case "!&" if current.nonEmpty => add(); current :+= line
        case "!=" => current :+= line
        case _ => add()
      }
    }

    add()

    groups.toSeq
  }

  private def parse(group: Seq[String]): (String, LinkSelectionCriteria) = {
    val cleaned = group.map(_.drop(2).trim)
    val name = cleaned.head
    val criteria = cleaned.tail.reverse.foldLeft[LinkSelectionCriteria](null) { case (current, line) =>
      val params = line.split("\\s+", 2)
      CriteriaParser.factories(params.head)(params.tail, current)
    }

    name -> criteria
  }

}

