package com.github.lucastorri.moca.role.master

import akka.actor.ActorRef
import com.github.lucastorri.moca.role.Work

case class State(ongoingWork: Map[ActorRef, Set[Work]]) {
  
  def start(who: ActorRef, work: Work): State = {
    val entry = who -> (get(who) + work)
    State(ongoingWork + entry)
  }

  def cancel(who: ActorRef, work: Work): State = {
    val entry = who -> (get(who) - work)
    State(ongoingWork + entry)
  }

  def cancel(who: ActorRef): State = {
    State(ongoingWork - who)
  }

  def get(who: ActorRef): Set[Work] = {
    ongoingWork.getOrElse(who, Set.empty)
  }

}

object State {

  def initial(): State =
    State(Map.empty)

}
