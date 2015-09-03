package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

class InMemWorkRepo(partition: PartitionSelector) extends WorkRepo with StrictLogging {

  private val workAdded = mutable.HashMap.empty[String, Work]
  private val workInProgress = mutable.HashMap.empty[String, Work]
  private val workDone = mutable.HashSet.empty[Work]
  private val runs = mutable.HashMap.empty[String, Run]
  private val results = mutable.HashMap.empty[String, mutable.ListBuffer[AllContentLinksTransfer]]

  override def available(): Future[Option[Task]] = {
    Future.successful(fromOpenTasks.orElse(fromAvailableWork))
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = {
    Future.successful(results.get(workId).flatMap(_.lastOption))
  }

  override def addTask(parentTaskId: String, linksDepth: Int, links: Set[Url]): Future[Unit] = {
    run(parentTaskId).newTasks(links, linksDepth)
    Future.successful(())
  }

  override def releaseAll(taskIds: Set[String]): Future[Unit] = {
    taskIds.foreach(release)
    Future.successful(())
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = {
    val r = run(taskId)
    r.complete(taskId, transfer)

    if (r.isDone) {
      runs.remove(r.id)
      workInProgress.remove(r.work.id)
      workDone.add(r.work)
      results.getOrElseUpdate(r.work.id, mutable.ListBuffer.empty).append(r.all)
    }

    Future.successful(if (r.isDone) Some(r.work.id) else None)
  }

  override def addWork(added: Set[Work]): Future[Boolean] = {
    val isNew = added.filter(work => !workInProgress.contains(work.id))
    isNew.foreach(work => workAdded.put(work.id, work))
    Future.successful(isNew.nonEmpty)
  }

  override def release(taskId: String): Future[Unit] = {
    run(taskId).release(taskId)
    Future.successful(())
  }


  private def run(taskId: String): Run = {
    val parts = taskId.split("::")
    runs(parts.head)
  }

  private class Run(val work: Work) {

    val id = Random.alphanumeric.take(16).mkString
    val open = mutable.HashMap.empty[String, Task]
    val inProgress = mutable.HashSet.empty[String]
    val depths = mutable.HashMap.empty[Url, Int]
    val links = mutable.ListBuffer.empty[ContentLink]

    logger.debug(s"New run for work ${work.id} ${work.seed} with task root id $id")
    newTasks(Set(work.seed), 0)

    def newTasks(urls: Set[Url], depth: Int): Unit = {
      urls.groupBy(partition.apply)
        .mapValues(group => group.filter(url => depth < depths.getOrElse(url, Int.MaxValue)))
        .filter { case (_, group) => group.nonEmpty }
        .foreach { case (part, group) =>
          val taskId = s"$id::${Random.alphanumeric.take(8).mkString}"
          val task = Task(taskId, group, work.criteria, depth, part)
          group.foreach(url => addDepth(url, depth))
          open.put(taskId, task)
          Run.allTasks.append(task)
          logger.debug(s"New task $taskId for work ${work.id}")
        }
    }

    def get(taskId: String): Unit = {
      open.remove(taskId)
      inProgress.add(taskId)
    }

    def release(taskId: String): Unit = {
      inProgress.remove(taskId)
      Run.allTasks.append(open(taskId))
    }

    def complete(taskId: String, transfer: ContentLinksTransfer): Unit = {
      inProgress.remove(taskId)
      open.remove(taskId)
      Run.release(taskId)
      transfer.contents.foreach { content =>
        addDepth(content.url, content.depth)
        links.append(content)
      }
    }

    private def addDepth(url: Url, depth: Int): Unit = {
      val existingDepthIsSmaller = depths.get(url).exists(_ < depth)
      if (!existingDepthIsSmaller) depths(url) = depth
    }

    def all: AllContentLinksTransfer =
      AllContentLinksTransfer(links)

    def isDone: Boolean =
      open.isEmpty

  }

  object Run {

    //TODO create a TaskScheduler to do this work
    private[Run] val allTasks = mutable.ListBuffer.empty[Task]
    private[Run] val lockedPartitions = mutable.HashSet.empty[String]
    private[Run] val taskPartition = mutable.HashMap.empty[String, String]

    def next(): Option[Task] = {
      allTasks.indexWhere(task => !lockedPartitions.contains(task.partition)) match {
        case -1 => None
        case n =>
          val task = allTasks.remove(n)
          lockedPartitions.add(task.partition)
          taskPartition.put(task.id, task.partition)
          Some(task)
      }
    }

    def release(taskId: String): Unit = {
      lockedPartitions.remove(taskPartition(taskId))
    }

  }

  private def fromOpenTasks: Option[Task] =
    Run.next()

  private def fromAvailableWork: Option[Task] = {
    workAdded.headOption.flatMap { case (id, work) =>
      workAdded.remove(id)
      workInProgress.put(id, work)
      val run = new Run(work)
      runs.put(run.id, run)
      Run.next()
    }
  }


}

case class AllContentLinksTransfer(all: Seq[ContentLink]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = all.toStream

}