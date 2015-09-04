package com.github.lucastorri.moca.collection

import java.util.Map.Entry

case class LRUCache[K, V](maxSize: Int) extends java.util.LinkedHashMap[K, V](16, 0.75f, true) {
  override def removeEldestEntry(eldest: Entry[K, V]): Boolean = size > maxSize
}