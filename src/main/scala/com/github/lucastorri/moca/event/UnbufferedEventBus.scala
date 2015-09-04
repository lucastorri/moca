package com.github.lucastorri.moca.event

import com.github.lucastorri.moca.event.EventBus.Topic

import scala.collection.mutable

class UnbufferedEventBus extends EventBus {

  private val subscribers = mutable.HashMap.empty[Topic[_], mutable.HashSet[Function[_, Unit]]]

  override def publish[T](topic: Topic[T], message: T): Boolean = {
    val group = subscribers.getOrElse(topic, Set.empty)
    group.foreach(subscriber => subscriber.asInstanceOf[Function[T, Unit]](message))
    group.nonEmpty
  }

  override def subscribe[T](topic: Topic[T])(f: (T) => Unit): Unit =
    subscribers.getOrElseUpdate(topic, mutable.HashSet.empty).add(f)

}
