package com.github.lucastorri.moca.store.scheduler

import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.store.work.TaskSubscriber

import scala.concurrent.Future

trait TaskScheduler extends TaskSubscriber {

   def next(): Future[Option[Task]]

   def release(taskIds: String*): Future[Unit]

   def close(): Unit

 }
