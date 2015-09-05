package com.github.lucastorri.moca.role.master.scheduler

import com.github.lucastorri.moca.role.Task

import scala.concurrent.duration.Deadline

trait TaskScheduler { self: Serializable =>

  def add(task: Task): TaskScheduler

  def next: Option[(Task, TaskScheduler)]

  def release(taskIds: Set[String]): TaskScheduler

}

case class OngoingTask(taskId: String, nextPing: Deadline) {

  override def equals(obj: Any): Boolean = obj match {
    case other: OngoingTask => other.taskId == taskId
    case _ => false
  }

  override def hashCode: Int = taskId.hashCode()

  def shouldPing: Boolean = nextPing.isOverdue()

}
