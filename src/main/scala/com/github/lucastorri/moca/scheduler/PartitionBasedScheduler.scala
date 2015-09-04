package com.github.lucastorri.moca.scheduler

import com.github.lucastorri.moca.role.Task
import com.github.lucastorri.moca.scheduler.PartitionBasedScheduler.State

class PartitionBasedScheduler extends TaskScheduler {

  private var state = State.initial()

  def add(task: Task): Unit = {
    state = state.add(task)
  }

  def next(): Option[Task] = {
    val next = state.nextTask
    state = state.nextState
    next
  }

  def release(taskIds: String*): Unit = {
    state = state.release(taskIds)
  }

}

object PartitionBasedScheduler {

  case class State(queues: Map[String, Seq[Task]], locked: Set[String], partitions: Map[String, String], scheduled: Seq[Task]) {

    def add(task: Task): State = {
      if (locked.contains(task.partition)) {
        val queue = queues.getOrElse(task.partition, Seq.empty) :+ task
        copy(queues = queues + (task.partition -> queue))
      } else {
        copy(locked = locked + task.partition, scheduled = scheduled :+ task)
      }
    }

    def nextState: State = {
      scheduled.headOption match {
        case Some(task) =>
          copy(scheduled = scheduled.tail, partitions = partitions + (task.id -> task.partition))
        case None =>
          this
      }
    }

    def nextTask: Option[Task] = {
      scheduled.headOption
    }

    def release(taskIds: Seq[String]): State = {
      var queuesCopy = queues
      var lockedCopy = locked
      var partitionsCopy = partitions
      var scheduledCopy = scheduled

      taskIds.foreach { taskId =>
        val partition = partitions(taskId)
        queues.get(partition) match {
          case Some(queue) =>
            scheduledCopy :+= queue.head
            val queueCopy = queue.tail

            queuesCopy =
              if (queueCopy.isEmpty) queuesCopy - partition
              else queuesCopy + (partition -> queueCopy)
          case None =>
            lockedCopy -= partition
        }

        partitionsCopy = partitionsCopy - taskId
      }

      copy(queues = queuesCopy, locked = lockedCopy, partitions = partitionsCopy, scheduled = scheduledCopy)
    }

  }

  object State {
    
    def initial(): State = 
      State(Map.empty, Set.empty, Map.empty, Seq.empty)

    def removeRepeatedTasks(state: State): State = {
      val repeated = (state.queues.values.flatten ++ state.scheduled)
        .groupBy(identity)
        .mapValues(_.size)
        .filter { case (task, appearances) => appearances > 1 }

      var queuesCopy = state.queues
      repeated.foreach { case (task, appearances) =>

        var queueCopy = queuesCopy(task.partition)
        val appearancesInQueue = queueCopy.count(_ == task)
        val taskIsScheduled = appearancesInQueue != appearances

        var toRemove =
          if (taskIsScheduled) appearancesInQueue
          else appearancesInQueue - 1

        def needsToRemove(): Boolean = {
          toRemove -= 1
          toRemove >= 0
        }

        queueCopy = queueCopy.reverse.filterNot(_ == task && needsToRemove()).reverse

        queuesCopy =
          if (queueCopy.isEmpty) queuesCopy - task.partition
          else queuesCopy + (task.partition -> queueCopy)
      }
      
      state.copy(queues = queuesCopy)
    }

  }

}