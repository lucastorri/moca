package com.github.lucastorri.moca.role.master.tasks

import scala.concurrent.duration.Deadline

case class OngoingTask(taskId: String, nextPing: Deadline) {

  override def equals(obj: Any): Boolean = obj match {
    case other: OngoingTask => other.taskId == taskId
    case _ => false
  }

  override def hashCode: Int = taskId.hashCode()

  def shouldPing: Boolean = nextPing.isOverdue()

}
