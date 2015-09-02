package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

//TODO it can end running two workers on the same host
class InMemWorkRepo(partition: PartitionSelector) extends WorkRepo with StrictLogging {

  private val workAdded = mutable.HashMap.empty[String, Work]
  private val workInProgress = mutable.HashMap.empty[String, Work]
  private val workDone = mutable.HashSet.empty[Work]
  private val runs = mutable.HashMap.empty[String, Run]
  private val results = mutable.HashMap.empty[String, mutable.ListBuffer[AllContentLinksTransfer]]

  override def available(): Future[Option[Task]] = {
    Future.successful(taskFromAvailableTasks.orElse(taskFromAvailableWork))
  }

  private def taskFromAvailableTasks: Option[Task] = {
    runs.find { case (_, run) => run.hasWork }.map { case (id, run) =>
      run.nextTask()
    }
  }

  private def taskFromAvailableWork: Option[Task] = {
    workAdded.headOption.map { case (id, work) =>
      workAdded.remove(id)
      workInProgress.put(id, work)
      val run = new Run(work)
      runs.put(run.id, run)
      run.nextTask()
    }
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
    val inProgress = mutable.HashMap.empty[String, Task]
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
          open.put(taskId, Task(taskId, group, work.criteria, depth, part))
          logger.debug(s"New task $taskId for work ${work.id}")
        }
    }

    def nextTask(): Task = {
      val (taskId, task) = open.head
      open.remove(taskId)
      inProgress.put(taskId, task)
      task
    }

    def release(taskId: String): Unit = {
      inProgress.remove(taskId).foreach(task => open.put(taskId, task))
    }

    def complete(taskId: String, transfer: ContentLinksTransfer): Unit = {
      inProgress.remove(taskId)
      transfer.contents.foreach { content =>
        val existingDepthIsSmaller = depths.get(content.url).exists(_ < content.depth)
        if (!existingDepthIsSmaller) depths(content.url) = content.depth
        links.append(content)
      }
    }

    def all: AllContentLinksTransfer =
      AllContentLinksTransfer(links)

    def isDone: Boolean =
      open.isEmpty && inProgress.isEmpty

    def hasWork: Boolean =
      open.nonEmpty

  }

}

case class AllContentLinksTransfer(all: Seq[ContentLink]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = all.toStream

}