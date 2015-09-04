package com.github.lucastorri.moca.scheduler

import com.github.lucastorri.moca.role.Task

import scala.concurrent.Future

trait TaskScheduler {

  def add(task: Task): Unit

  def next(): Future[Option[Task]]

  def release(taskIds: String*): Unit

}
