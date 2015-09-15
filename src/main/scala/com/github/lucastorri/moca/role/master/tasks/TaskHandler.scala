package com.github.lucastorri.moca.role.master.tasks

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.master.Master

import scala.concurrent.duration.Deadline

case class TaskHandler(ongoing: Map[ActorRef, Set[OngoingTask]]) {

  def ongoingTasks(): Map[ActorRef, Set[OngoingTask]] =
    ongoing

  def ongoingTasks(who: ActorRef): Set[OngoingTask] =
    ongoing.getOrElse(who, Set.empty)

  def start(who: ActorRef, taskId: String): TaskHandler = {
    val entry = who -> (ongoingTasks(who) + create(taskId))
    copy(ongoing = ongoing + entry)
  }

  def cancel(who: ActorRef, taskId: String): TaskHandler = {
    val taskSet = ongoingTasks(who) - create(taskId)
    val clean =
      if (taskSet.isEmpty) ongoing - who
      else ongoing + (who -> taskSet)
    copy(ongoing = clean)
  }

  def cancel(who: ActorRef): TaskHandler =
    copy(ongoing = ongoing - who)

  def extendDeadline(toExtend: Map[ActorRef, Set[String]]): TaskHandler = {
    val updates = toExtend.map { case (who, tasks) =>
      val ongoing = ongoingTasks(who)
      who -> tasks.flatMap { taskId =>
        val ot = create(taskId)
        if (ongoing.contains(ot)) Some(ot) else None
      }
    }.filter { case (who, tasks) =>
      tasks.nonEmpty
    }
    copy(ongoing = ongoing ++ updates)
  }

  def done(who: ActorRef, taskId: String): TaskHandler =
    cancel(who, taskId)

  private def create(taskId: String): OngoingTask =
    OngoingTask(taskId, Deadline.now + Master.pingInterval)
  
}

object TaskHandler {

  def initial(): TaskHandler =
    TaskHandler(Map.empty)

}
