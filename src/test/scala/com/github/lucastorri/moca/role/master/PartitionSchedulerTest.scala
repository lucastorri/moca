package com.github.lucastorri.moca.role.master

import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.role.master.scheduler.PartitionScheduler
import com.github.lucastorri.moca.url.Url
import org.scalatest.{FlatSpec, MustMatchers}

class PartitionSchedulerTest extends FlatSpec with MustMatchers {

  it must "keep only the scheduled task when removing duplicates" in {

    val repeated1 = task(1, "p1")
    val repeated2 = task(2, "p2")

    val state = PartitionScheduler.initial
      .add(repeated1)
      .add(task(3, "p1"))
      .add(task(4, "p2"))
      .add(repeated2)
      .add(task(5, "p3"))
      .add(task(6, "p3"))
      .add(repeated1)
      .add(repeated2)

    PartitionScheduler.removeRepeatedTasks(state) must equal (
      PartitionScheduler.initial
        .add(repeated1)
        .add(task(3, "p1"))
        .add(task(4, "p2"))
        .add(repeated2)
        .add(task(5, "p3"))
        .add(task(6, "p3"))
    )

  }

  it must "keep only the first task on the partition queue when removing duplicates" in {

    val repeated1 = task(1, "p1")

    val state = PartitionScheduler.initial
      .add(task(2, "p1"))
      .add(task(3, "p2"))
      .add(repeated1)
      .add(task(4, "p1"))
      .add(repeated1)

    PartitionScheduler.removeRepeatedTasks(state) must equal (
      PartitionScheduler.initial
        .add(task(2, "p1"))
        .add(task(3, "p2"))
        .add(repeated1)
        .add(task(4, "p1"))
    )

  }

  it must "return and release tasks" in {
    val FirstTask = task(1, "p1")

    val state = PartitionScheduler.initial
      .add(FirstTask)
      .add(task(2, "p1"))
      .add(task(3, "p2"))
      .add(task(4, "p2"))

    state.all must equal (Set("1", "2", "3", "4"))

    val Some((FirstTask, afterNext)) = state.next
    afterNext.queues must equal (Map("p1" -> Seq(task(2, "p1")), "p2" -> Seq(task(4, "p2"))))
    afterNext.locked must equal (Set("p1", "p2"))
    afterNext.scheduled must equal (Seq(task(3, "p2")))
    afterNext.partitions must equal (Map("1" -> "p1"))
    afterNext.all must equal (Set("1", "2", "3", "4"))

    val afterRelease = afterNext.release(Set("1"))
    afterRelease.queues must equal (Map("p2" -> Seq(task(4, "p2"))))
    afterRelease.locked must equal (Set("p1", "p2"))
    afterRelease.scheduled must equal (Seq(task(3, "p2"), task(2, "p1")))
    afterRelease.partitions must equal (Map.empty)
    afterRelease.all must equal(Set("2", "3", "4"))

    val after3 = afterRelease.next.get._2.next.get._2.release(Set("2", "3"))
    after3.queues must equal (Map.empty)
    after3.locked must equal (Set("p2"))
    after3.scheduled must equal (Seq(task(4, "p2")))
    after3.partitions must equal (Map.empty)

    val afterAll = after3.next.get._2.release(Set("4"))
    afterAll must equal (PartitionScheduler.initial)
  }

  it must "not re-add tasks" in {
    val task1 = task(1, "p1")
    val task2 = task(2, "p1")

    val state = PartitionScheduler.initial
      .add(task1)
      .add(task2)

    state.add(task1).add(task2) must equal (state)
  }

  it must "not care about unknown ids released" in {

    val state = PartitionScheduler.initial
      .add(task(1, "p1"))
      .add(task(2, "p1"))
      .next.get._2

    state.release(Set("unknown")) must equal (state)
  }


  def task(id: Int, partition: String): Task =
    Task(id.toString, Set(Url(s"http://seed$id.com")), null, 0, partition)

}
