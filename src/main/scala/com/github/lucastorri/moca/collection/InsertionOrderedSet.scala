package com.github.lucastorri.moca.collection

import com.github.lucastorri.moca.collection.InsertionOrderedSet.{DefaultPointer, Entry, Pointer}

class InsertionOrderedSet[T]()
  extends scala.collection.mutable.Set[T]
  with scala.collection.mutable.SetLike[T, InsertionOrderedSet[T]] {

  private val _set = innerMap()
  private val _head = headPointer()
  private val _last = lastPointer()

  override def contains(item: T): Boolean = _set.containsKey(item.hashCode)

  override def size: Int = _set.size

  override def isEmpty: Boolean = _set.isEmpty

  override def empty: InsertionOrderedSet[T] = new InsertionOrderedSet[T]()

  override def add(item: T): Boolean = {
    val removed = remove(item)
    val k = item.hashCode

    if (isEmpty) {
      _head.v = k
      _last.v = k
      _set.put(k, Entry(item, k, k))
    } else if (_head == _last) {
      _set.put(k, Entry(item, _last.v, _last.v))
      _set.put(_last.v, _set.get(_last.v).copy(previous = k, next = k))
      _last.v = k
    } else {
      _set.put(k, Entry(item, _last.v, _head.v))
      _set.put(_head.v, _set.get(_head.v).copy(previous = k))
      _set.put(_last.v, _set.get(_last.v).copy(next = k))
      _last.v = k
    }

    onUpdate()
    !removed
  }

  override def remove(item: T): Boolean = {
    val k = item.hashCode
    Option(_set.remove(k)).exists { old =>

      if (nonEmpty) {
        _set.put(old.previous, _set.get(old.previous).copy(next = old.next))
        _set.put(old.next, _set.get(old.next).copy(previous = old.previous))
      }

      if (k == _head.v) {
        _head.v = old.next
      } else if (k == _last.v) {
        _last.v = old.previous
      }

      onUpdate()
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

    private val first = _head.v
    private var _next: Int = _head.v
    private var _hasNext = InsertionOrderedSet.this.nonEmpty

    override def hasNext: Boolean = _hasNext

    override def next(): T = {
      val entry = _set.get(_next)
      if (entry.next == first) _hasNext = false
      _next = entry.next

      entry.item
    }

  }

  protected def innerMap(): java.util.Map[Int, Entry[T]] = new java.util.HashMap[Int, Entry[T]]()

  protected def headPointer(): Pointer = new DefaultPointer
  
  protected def lastPointer(): Pointer = new DefaultPointer

  protected def onUpdate(): Unit = {}

  override def toString(): String = iterator.mkString("{", ", ", "}")

}

object InsertionOrderedSet {

  def empty[T]: InsertionOrderedSet[T] = new InsertionOrderedSet[T]()

  trait Pointer {
    def v: Int
    def v_=(i: Int): Unit
  }

  class DefaultPointer extends Pointer {
    var v = 0
  }

  case class Entry[T](item: T, previous: Int, next: Int)

}