package com.github.lucastorri.moca.collection

import org.scalatest.{MustMatchers, FlatSpec}

class InsertionOrderedSetTest extends FlatSpec with MustMatchers {

  it must "keep insertion order" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(3)
    set.add(1)
    set.add(2)
    set.add(4)

    set.toSeq must equal (Seq(3, 1, 2, 4))

  }

  it must "allow removal of elements" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(1)
    set.add(2)
    set.add(3)

    set.remove(1) must be (true)
    set.remove(3) must be (true)
    set.remove(2) must be (true)

    set.remove(4) must be (false)

  }

  it must "know if an item is on the set" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(1)
    set.add(3)

    set.contains(1) must be (true)
    set.contains(3) must be (true)

    set.contains(5) must be (false)

  }

  it must "allow updates to the first and last element" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(1) must be (true)
    set.add(2) must be (true)
    set.add(3) must be (true)

    set.add(1) must be (false)
    set.add(3) must be (false)

    set.toSeq must equal (Seq(2, 1, 3))

  }

  it must "move some item to last on update" in {

    val set = InsertionOrderedSet.empty[Int]

    set.add(3)
    set.add(1)
    set.add(2)

    set.add(1)

    set.toSeq must equal (Seq(3, 2, 1))

  }

  it must "known when it's empty and its size" in {

    val set = InsertionOrderedSet.empty[Int]

    set must be ('empty)
    set must have size 0

    set.add(3)
    set must be ('nonEmpty)
    set must have size 1

    set.add(7)
    set must be ('nonEmpty)
    set must have size 2

    set.remove(3)
    set.remove(7)
    set must be ('empty)
    set must have size 0

  }

  it must "be equal on different orders" in {

    val set1 = InsertionOrderedSet.empty[Int]
    val set2 = InsertionOrderedSet.empty[Int]

    set1.add(1)
    set1.add(2)

    set2.add(2)
    set2.add(1)

    set1 must equal (set2)
    set1.hashCode() must equal (set2.hashCode())

  }

  it must "be different on different items" in {

    val set1 = InsertionOrderedSet.empty[Int]
    val set2 = InsertionOrderedSet.empty[Int]

    set1.add(1)
    set1.add(2)

    set2.add(2)
    set2.add(3)

    set1 must not equal set2
    set1.hashCode() must not equal set2.hashCode()

  }

}
