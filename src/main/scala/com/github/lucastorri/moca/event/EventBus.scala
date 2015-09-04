package com.github.lucastorri.moca.event

import com.github.lucastorri.moca.event.EventBus.Topic
import com.github.lucastorri.moca.role.Task

trait EventBus {
  
  def publish[T](topic: Topic[T], message: T): Boolean

  def subscribe[T](topic: Topic[T])(f: T => Unit): Unit

}

object EventBus {

  sealed trait Topic[T]
  case object NewTasks extends Topic[Task]

}