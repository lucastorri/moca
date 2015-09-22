package com.github.lucastorri.moca.collection

import com.github.lucastorri.moca.collection.InsertionOrderedSet.Entry

class InsertionOrderedSet[T]()
  extends scala.collection.mutable.Set[T]
  with scala.collection.mutable.SetLike[T, InsertionOrderedSet[T]] {

  private val _set = emptyInnerMap()
  private var _head = 0
  private var _last = 0

  override def contains(item: T): Boolean = _set.containsKey(item.hashCode)

  override def size: Int = _set.size

  override def isEmpty: Boolean = _set.isEmpty

  override def empty: InsertionOrderedSet[T] = new InsertionOrderedSet[T]()

  override def add(item: T): Boolean = {
    val removed = remove(item)
    val k = item.hashCode

    if (isEmpty) {
      _head = k
      _last = k
      _set.put(k, Entry(item, k, k))
    } else if (_head == _last) {
      _set.put(k, Entry(item, _last, _last))
      _set.put(_last, _set.get(_last).copy(previous = k, next = k))
      _last = k
    } else {
      _set.put(k, Entry(item, _last, _head))
      _set.put(_head, _set.get(_head).copy(previous = k))
      _set.put(_last, _set.get(_last).copy(next = k))
      _last = k
    }

    !removed
  }

  override def remove(item: T): Boolean = {
    val k = item.hashCode
    Option(_set.remove(k)).exists { old =>

      if (nonEmpty) {
        _set.put(old.previous, _set.get(old.previous).copy(next = old.next))
        _set.put(old.next, _set.get(old.next).copy(previous = old.previous))
      }

      if (k == _head) {
        _head = old.next
      } else if (k == _last) {
        _last = old.previous
      }

      true
    }
  }

  override def +=(item: T): InsertionOrderedSet.this.type = {
    add(item)
    this
  }

  override def -=(item: T): InsertionOrderedSet.this.type = {
    remove(item)
    this
  }

  override def iterator: Iterator[T] = new Iterator[T] {

    private val first = _head
    private var _next: Int = _head
    private var _hasNext = InsertionOrderedSet.this.nonEmpty

    override def hasNext: Boolean = _hasNext

    override def next(): T = {
      val entry = _set.get(_next)
      if (entry.next == first) _hasNext = false
      _next = entry.next

      entry.item
    }

  }

  def emptyInnerMap(): java.util.Map[Int, Entry[T]] = new java.util.HashMap[Int, Entry[T]]()

  override def toString(): String = iterator.mkString("{", ", ", "}")

}

object InsertionOrderedSet {

  def empty[T]: InsertionOrderedSet[T] = new InsertionOrderedSet[T]()

  case class Entry[T](item: T, previous: Int, next: Int)

}