package com.github.lucastorri.moca.scheduler

import com.github.lucastorri.moca.role.Task

trait TaskScheduler {

  def add(task: Task): Unit

  def next(): Option[Task]

  def release(taskIds: String*): Unit

}
