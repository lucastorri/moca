package com.github.lucastorri.moca.role.master.scheduler

import com.github.lucastorri.moca.role.Task

case class PartitionScheduler(
  queues: Map[String, Seq[Task]], locked: Set[String], 
  partitions: Map[String, String], scheduled: Seq[Task], all: Set[String]) {

  def add(task: Task): PartitionScheduler = {
    if (all.contains(task.id)) {
      this
    } else if (locked.contains(task.partition)) {
      val queue = queues.getOrElse(task.partition, Seq.empty) :+ task
      copy(queues = queues + (task.partition -> queue), all = all + task.id)
    } else {
      copy(locked = locked + task.partition, scheduled = scheduled :+ task, all = all + task.id)
    }
  }

  def next: Option[(Task, PartitionScheduler)] = {
    scheduled.headOption.map { task =>
      task -> copy(scheduled = scheduled.tail, partitions = partitions + (task.id -> task.partition))
    }
  }

  def release(taskIds: Set[String]): PartitionScheduler = {
    var queuesCopy = queues
    var lockedCopy = locked
    var partitionsCopy = partitions
    var scheduledCopy = scheduled

    taskIds.filter(partitions.contains).foreach { taskId =>
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

    copy(queues = queuesCopy, locked = lockedCopy, partitions = partitionsCopy,
      scheduled = scheduledCopy, all = all -- taskIds)
  }

}

object PartitionScheduler {

  def initial(): PartitionScheduler =
    PartitionScheduler(Map.empty, Set.empty, Map.empty, Seq.empty, Set.empty)

  def removeRepeatedTasks(state: PartitionScheduler): PartitionScheduler = {
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
