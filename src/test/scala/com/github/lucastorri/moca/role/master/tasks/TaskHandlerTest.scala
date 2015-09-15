package com.github.lucastorri.moca.role.master.tasks

import akka.actor.ActorRef
import org.scalatest.{MustMatchers, FlatSpec}

class TaskHandlerTest extends FlatSpec with MustMatchers {

  it must "return an worker tasks" in {

    val state = TaskHandler.initial()
      .start(ActorRef.noSender, "task-1")

    state.ongoingTasks(ActorRef.noSender) must equal (Set(OngoingTask("task-1", null)))

  }

  it must "return empty set if worker has no tasks" in {

    TaskHandler.initial().ongoingTasks(ActorRef.noSender) must be (Set.empty)

  }

  it must "remove tasks on cancel" in {

    val state = TaskHandler.initial()
      .start(ActorRef.noSender, "task-1")
      .start(ActorRef.noSender, "task-2")

    val cancelFirst = state.cancel(ActorRef.noSender, "task-1")
    val cancelBoth = state.cancel(ActorRef.noSender, "task-2").cancel(ActorRef.noSender, "task-1")
    val cancelAll = state.cancel(ActorRef.noSender)

    cancelFirst.ongoingTasks(ActorRef.noSender) must be (Set(OngoingTask("task-2", null)))

    cancelBoth.ongoingTasks() must not contain key (ActorRef.noSender)
    cancelAll.ongoingTasks() must not contain key (ActorRef.noSender)

  }

  it must "remove tasks on done" in {

    val state = TaskHandler.initial()
      .start(ActorRef.noSender, "task-1")
      .start(ActorRef.noSender, "task-2")

    val doneFirst = state.done(ActorRef.noSender, "task-1")
    val doneBoth = state.done(ActorRef.noSender, "task-2").done(ActorRef.noSender, "task-1")

    doneFirst.ongoingTasks(ActorRef.noSender) must be (Set(OngoingTask("task-2", null)))
    doneBoth.ongoingTasks() must not contain key (ActorRef.noSender)

  }

  it must "not do anything if a task cancelled or done doesn't exist" in {

    TaskHandler.initial().cancel(ActorRef.noSender).ongoingTasks() must not contain key (ActorRef.noSender)
    TaskHandler.initial().cancel(ActorRef.noSender, "task-x").ongoingTasks() must not contain key (ActorRef.noSender)
    TaskHandler.initial().done(ActorRef.noSender, "task-x").ongoingTasks() must not contain key (ActorRef.noSender)

  }

  it must "extend tasks deadlines" in {

    val state = TaskHandler.initial().start(ActorRef.noSender, "task-1")

    val running = state.ongoingTasks(ActorRef.noSender).head
    Thread.sleep(10)
    val extended = state.extendDeadline(Map(ActorRef.noSender -> Set("task-1"))).ongoingTasks(ActorRef.noSender).head

    extended.nextPing > running.nextPing must be (true)

  }

  it must "not extended the deadline for tasks that don't exist" in {

    val afterExtension = TaskHandler.initial().extendDeadline(Map(ActorRef.noSender -> Set("task-1")))

    afterExtension.ongoingTasks() must not contain key (ActorRef.noSender)

  }

}
