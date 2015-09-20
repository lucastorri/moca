package com.github.lucastorri.moca.role.master.scheduler

import com.github.lucastorri.moca.role.Task

case class PartitionScheduler(
  queues: Map[String, Seq[Task]], locked: Set[String], 
  partitions: Map[String, String], scheduled: Seq[Task], all: Set[String]) {

  def add(tasks: Set[Task]): PartitionScheduler = {
    var queuesCopy = queues
    var allCopy = all
    var lockedCopy = locked
    var scheduledCopy = scheduled

    tasks.foreach { task =>
      if (!allCopy.contains(task.id)) {
        if (lockedCopy.contains(task.partition)) {
          val queue = queuesCopy.getOrElse(task.partition, Seq.empty) :+ task
          queuesCopy = queuesCopy + (task.partition -> queue)
          allCopy = allCopy + task.id
        } else {
          lockedCopy = lockedCopy + task.partition
          scheduledCopy = scheduledCopy :+ task
          allCopy = allCopy + task.id
        }
      }
    }

    copy(queues = queuesCopy, all = allCopy, locked = lockedCopy, scheduled = scheduledCopy)
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
