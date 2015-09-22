package com.github.lucastorri.moca.collection

import org.scalatest.{MustMatchers, FlatSpec}

import scala.collection.mutable

class LRUCacheTest extends FlatSpec with MustMatchers {

  it must "remove items on overflow" in {

    val overflow = mutable.ListBuffer.empty[(String, Int)]

    val cache = LRUCache.withAction[String, Int](5) { (k, v) =>
      overflow += (k -> v)
    }

    cache.put("1", 1)
    cache.put("2", 2)
    cache.put("3", 3)
    cache.put("4", 4)
    cache.put("5", 5)

    overflow must have size 0

    cache.put("6", 6)
    overflow must equal (Seq("1" -> 1))

    cache.put("2", 8)
    overflow must equal (Seq("1" -> 1))

    cache.put("7", 7)
    overflow must equal (Seq("1" -> 1, "3" -> 3))

  }

}
