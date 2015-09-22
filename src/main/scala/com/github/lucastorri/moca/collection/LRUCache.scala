package com.github.lucastorri.moca.collection

import java.util.Map.Entry

case class LRUCache[K, V](maxSize: Int, onRemoval: (K, V) => Unit) extends java.util.LinkedHashMap[K, V](16, 0.75f, true) {

  override def removeEldestEntry(eldest: Entry[K, V]): Boolean = {
    if (size <= maxSize) false
    else {
      onRemoval(eldest.getKey, eldest.getValue)
      true
    }
  }

}

object LRUCache {

  def ofSize[K, V](maxSize: Int): LRUCache[K, V] =
    LRUCache(maxSize, (_, _) => ())

  def withAction[K, V](maxSize: Int)(onRemoval: (K, V) => Unit): LRUCache[K, V] =
    LRUCache(maxSize, onRemoval)

}
