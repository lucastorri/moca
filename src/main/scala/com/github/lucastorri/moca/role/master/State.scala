package com.github.lucastorri.moca.role.master

import akka.actor.ActorRef

import scala.concurrent.duration.Deadline

case class State(ongoingWork: Map[ActorRef, Set[OngoingWork]]) {

  def start(who: ActorRef, workId: String): State = {
    val entry = who -> (get(who) + create(workId))
    copy(ongoingWork = ongoingWork + entry)
  }

  def cancel(who: ActorRef, workId: String): State = {
    val workSet = get(who) - create(workId)
    val ongoing =
      if (workSet.isEmpty) ongoingWork - who
      else ongoingWork + (who -> workSet)
    copy(ongoingWork = ongoing)
  }

  def cancel(who: ActorRef): State =
    copy(ongoingWork = ongoingWork - who)

  def get(who: ActorRef): Set[OngoingWork] =
    ongoingWork.getOrElse(who, Set.empty)

  def extendDeadline(toExtend: Map[ActorRef, Set[OngoingWork]]): State = {
    val updates = toExtend.mapValues(_.map(ongoing => create(ongoing.workId)))
    copy(ongoingWork = ongoingWork ++ updates)
  }

  def done(who: ActorRef, workId: String): State =
    cancel(who, workId)

  private def create(workId: String): OngoingWork =
    OngoingWork(workId, Deadline.now + Master.pingInterval)

}

object State {

  def initial(): State =
    State(Map.empty)

}

case class OngoingWork(workId: String, nextPing: Deadline) {

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: OngoingWork => other.workId == workId
    case _ => false
  }

  override def hashCode: Int = workId.hashCode()

  def shouldPing: Boolean = nextPing.isOverdue()

}
