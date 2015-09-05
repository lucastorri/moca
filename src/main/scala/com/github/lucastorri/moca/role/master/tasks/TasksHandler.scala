package com.github.lucastorri.moca.role.master.tasks

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.master.scheduler.OngoingTask

trait TasksHandler { self: Serializable =>

  def ongoingTasks(): Map[ActorRef, Set[OngoingTask]]

  def ongoingTasks(who: ActorRef): Set[OngoingTask]

  def start(who: ActorRef, taskId: String): TasksHandler

  def cancel(who: ActorRef, taskId: String): TasksHandler

  def cancel(who: ActorRef): TasksHandler

  def extendDeadline(toExtend: Map[ActorRef, Set[OngoingTask]]): TasksHandler

  def done(who: ActorRef, taskId: String): TasksHandler

}
