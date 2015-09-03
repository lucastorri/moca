package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Task

import scala.concurrent.Future

trait TaskSubscriber {

  def add(task: Task): Future[Unit]

}
