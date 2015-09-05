package com.github.lucastorri.moca.role.master.tasks

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.master.Master
import com.github.lucastorri.moca.role.master.scheduler.OngoingTask

import scala.concurrent.duration.Deadline

case class BasicTasksHandler(ongoing: Map[ActorRef, Set[OngoingTask]]) extends TasksHandler {

  override def ongoingTasks(): Map[ActorRef, Set[OngoingTask]] =
    ongoing

  override def ongoingTasks(who: ActorRef): Set[OngoingTask] =
    ongoing.getOrElse(who, Set.empty)

  override def start(who: ActorRef, taskId: String): BasicTasksHandler = {
    val entry = who -> (ongoingTasks(who) + create(taskId))
    copy(ongoing = ongoing + entry)
  }

  override def cancel(who: ActorRef, taskId: String): BasicTasksHandler = {
    val taskSet = ongoingTasks(who) - create(taskId)
    val clean =
      if (taskSet.isEmpty) ongoing - who
      else ongoing + (who -> taskSet)
    copy(ongoing = clean)
  }

  override def cancel(who: ActorRef): BasicTasksHandler =
    copy(ongoing = ongoing - who)

  override def extendDeadline(toExtend: Map[ActorRef, Set[OngoingTask]]): BasicTasksHandler = {
    val updates = toExtend.mapValues(_.map(ongoing => create(ongoing.taskId)))
    copy(ongoing = ongoing ++ updates)
  }

  override def done(who: ActorRef, taskId: String): BasicTasksHandler =
    cancel(who, taskId)

  private def create(taskId: String): OngoingTask =
    OngoingTask(taskId, Deadline.now + Master.pingInterval)
  
}

object BasicTasksHandler {

  def initial(): BasicTasksHandler =
    BasicTasksHandler(Map.empty)

}

class BasicTaskHandlerCreator extends TasksHandlerCreator {

  override def apply(): TasksHandler =
    BasicTasksHandler.initial()

}