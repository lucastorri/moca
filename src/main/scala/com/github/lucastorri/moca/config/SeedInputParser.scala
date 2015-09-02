package com.github.lucastorri.moca.config

import java.io.File
import java.nio.file.Files

import com.github.lucastorri.moca.role.Work
import com.github.lucastorri.moca.url.Url

import scala.collection.JavaConverters._

object SeedInputParser {

  def fromString(str: String): Seq[Work] =
    fromLines(str.lines)

  def fromFile(file: File): Seq[Work] =
    fromLines(Files.readAllLines(file.toPath).asScala)

  def fromLines(lines: Iterable[String]): Seq[Work] =
    fromLines(lines.iterator)

  def fromLines(lines: Iterator[String]): Seq[Work] = {
    val (criteriaLines, seedLines) = lines.map(_.trim).filter(_.nonEmpty).toList.partition(_.startsWith("!"))
    val criteria = CriteriaParser.fromLines(criteriaLines)

    seedLines.map { line =>
      val Array(url, id, name) = line.replaceAll("\\|", " | ").split("\\|").map(_.trim).padTo(3, "")
      val seed = Url(url)
      val selectedId = if (id.nonEmpty) id else seed.id
      Work(selectedId, seed, criteria(name))
    }
  }

}

