package com.github.lucastorri.moca.collection

import com.github.lucastorri.moca.collection.InsertionOrderedSet.{Entry, Pointer}
import org.mapdb.{Atomic, DB}

class MapDBInsertionOrderedSet[T](db: DB) extends InsertionOrderedSet[T] {

  override protected def innerMap(): java.util.Map[Int, Entry[T]] = db.hashMap[Int, Entry[T]]("entries")

  override protected def headPointer(): Pointer = MapDBPointer(db.atomicInteger("head"))

  override protected def lastPointer(): Pointer = MapDBPointer(db.atomicInteger("last"))

  override protected def onUpdate(): Unit = db.commit()

  case class MapDBPointer(int: Atomic.Integer) extends Pointer {

    override def v: Int = int.get()

    override def v_=(i: Int): Unit = int.set(i)

  }

}
