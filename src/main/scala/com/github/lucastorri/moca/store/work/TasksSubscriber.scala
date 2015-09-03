package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.role.Task

trait TasksSubscriber {

  def newTask(task: Task): Unit

}
