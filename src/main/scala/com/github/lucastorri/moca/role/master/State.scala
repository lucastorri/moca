package com.github.lucastorri.moca.role.master

import akka.actor.ActorRef

import scala.concurrent.duration.Deadline

case class State(ongoingTasks: Map[ActorRef, Set[OngoingTask]]) {

  def start(who: ActorRef, taskId: String): State = {
    val entry = who -> (get(who) + create(taskId))
    copy(ongoingTasks = ongoingTasks + entry)
  }

  def cancel(who: ActorRef, taskId: String): State = {
    val taskSet = get(who) - create(taskId)
    val ongoing =
      if (taskSet.isEmpty) ongoingTasks - who
      else ongoingTasks + (who -> taskSet)
    copy(ongoingTasks = ongoing)
  }

  def cancel(who: ActorRef): State =
    copy(ongoingTasks = ongoingTasks - who)

  def get(who: ActorRef): Set[OngoingTask] =
    ongoingTasks.getOrElse(who, Set.empty)

  def extendDeadline(toExtend: Map[ActorRef, Set[OngoingTask]]): State = {
    val updates = toExtend.mapValues(_.map(ongoing => create(ongoing.taskId)))
    copy(ongoingTasks = ongoingTasks ++ updates)
  }

  def done(who: ActorRef, taskId: String): State =
    cancel(who, taskId)

  private def create(taskId: String): OngoingTask =
    OngoingTask(taskId, Deadline.now + Master.pingInterval)

}

object State {

  def initial(): State =
    State(Map.empty)

}

case class OngoingTask(taskId: String, nextPing: Deadline) {

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: OngoingTask => other.taskId == taskId
    case _ => false
  }

  override def hashCode: Int = taskId.hashCode()

  def shouldPing: Boolean = nextPing.isOverdue()

}
