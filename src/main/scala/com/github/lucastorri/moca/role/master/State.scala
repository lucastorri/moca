package com.github.lucastorri.moca.role.master

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.Work

import scala.concurrent.duration.Deadline

case class State(ongoingWork: Map[ActorRef, Set[OngoingWork]]) {
  
  def start(who: ActorRef, work: Work): State = {
    val entry = who -> (get(who) + create(work))
    copy(ongoingWork = ongoingWork + entry)
  }

  def cancel(who: ActorRef, work: Work): State = {
    val entry = who -> (get(who) - create(work))
    copy(ongoingWork = ongoingWork + entry)
  }

  def cancel(who: ActorRef): State =
    copy(ongoingWork = ongoingWork - who)

  def get(who: ActorRef): Set[OngoingWork] =
    ongoingWork.getOrElse(who, Set.empty)

  def extendDeadline(toExtend: Map[ActorRef, Set[OngoingWork]]): State = {
    val updates = toExtend.mapValues(_.map(ongoing => create(ongoing.work)))
    copy(ongoingWork = ongoingWork ++ updates)
  }

  private def create(work: Work): OngoingWork =
    OngoingWork(work, Deadline.now + Master.pingInterval)

}

object State {

  def initial(): State =
    State(Map.empty)

}

case class OngoingWork(work: Work, nextPing: Deadline) {

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: OngoingWork => other.work == work
    case _ => false
  }

  override def hashCode: Int = work.hashCode()

  def shouldPing: Boolean = nextPing.isOverdue()

}
