package com.github.lucastorri.moca.collection

import org.scalatest.{MustMatchers, FlatSpec}

class InsertionOrderedSetTest extends FlatSpec with MustMatchers {

  it must "add items and keep the insertion order" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(3)
    set.add(1)
    set.add(2)
    set.add(4)

    set.toSeq must equal (Seq(3, 1, 2, 4))
  }

}
