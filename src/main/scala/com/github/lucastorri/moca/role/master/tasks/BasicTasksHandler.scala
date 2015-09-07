package com.github.lucastorri.moca.role.master.tasks

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.master.Master

import scala.concurrent.duration.Deadline

case class BasicTasksHandler(ongoing: Map[ActorRef, Set[OngoingTask]]) {

  def ongoingTasks(): Map[ActorRef, Set[OngoingTask]] =
    ongoing

  def ongoingTasks(who: ActorRef): Set[OngoingTask] =
    ongoing.getOrElse(who, Set.empty)

  def start(who: ActorRef, taskId: String): BasicTasksHandler = {
    val entry = who -> (ongoingTasks(who) + create(taskId))
    copy(ongoing = ongoing + entry)
  }

  def cancel(who: ActorRef, taskId: String): BasicTasksHandler = {
    val taskSet = ongoingTasks(who) - create(taskId)
    val clean =
      if (taskSet.isEmpty) ongoing - who
      else ongoing + (who -> taskSet)
    copy(ongoing = clean)
  }

  def cancel(who: ActorRef): BasicTasksHandler =
    copy(ongoing = ongoing - who)

  def extendDeadline(toExtend: Map[ActorRef, Set[OngoingTask]]): BasicTasksHandler = {
    val updates = toExtend.mapValues(_.map(ongoing => create(ongoing.taskId)))
    copy(ongoing = ongoing ++ updates)
  }

  def done(who: ActorRef, taskId: String): BasicTasksHandler =
    cancel(who, taskId)

  private def create(taskId: String): OngoingTask =
    OngoingTask(taskId, Deadline.now + Master.pingInterval)
  
}

object BasicTasksHandler {

  def initial(): BasicTasksHandler =
    BasicTasksHandler(Map.empty)

}
