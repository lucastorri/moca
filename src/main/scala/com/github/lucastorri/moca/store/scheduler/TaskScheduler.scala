package com.github.lucastorri.moca.store.scheduler

import com.github.lucastorri.moca.role.Task

import scala.concurrent.Future

trait TaskScheduler {

  def add(task: Task): Future[Unit]

  def next(): Future[Option[Task]]

  def release(taskIds: String*): Future[Unit]

  def close(): Unit

}
