package com.github.lucastorri.moca.store.work

import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.scheduler.TaskScheduler
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Random

class InMemWorkRepo(partition: PartitionSelector, scheduler: TaskScheduler) extends WorkRepo with StrictLogging {

  private val works = mutable.HashMap.empty[String, Work]
  private val results = mutable.HashMap.empty[String, mutable.ListBuffer[AllContentLinksTransfer]]


  override def addWork(added: Set[Work]): Future[Boolean] = {
    val isNew = added.filterNot(work => works.contains(work.id))
    isNew.foreach { work =>
      works.put(work.id, work)
      Run.createFor(work).initialTasks.foreach(scheduler.add)
    }
    Future.successful(isNew.nonEmpty)
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = {
    Future.successful(results.get(workId).flatMap(_.lastOption))
  }

  override def addTask(parentTaskId: String, linksDepth: Int, links: Set[Url]): Future[Unit] = {
    Run.forId(parentTaskId).newTasks(links, linksDepth).foreach(scheduler.add)
    Future.successful(())
  }

  override def release(taskId: String): Future[Unit] = {
    Run.forId(taskId).release(taskId).foreach(scheduler.add)
    Future.successful(())
  }

  override def releaseAll(taskIds: Set[String]): Future[Unit] = {
    taskIds.foreach(release)
    Future.successful(())
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = {
    val r = Run.forId(taskId)
    r.complete(taskId, transfer)

    if (r.isDone) {
      results.getOrElseUpdate(r.work.id, mutable.ListBuffer.empty).append(r.transfers)
      r.close()
    }

    Future.successful(if (r.isDone) Some(r.work.id) else None)
  }


  private class Run private[Run](val work: Work) {

    val id = Random.alphanumeric.take(16).mkString
    val tasks = mutable.HashMap.empty[String, Task]
    val depths = mutable.HashMap.empty[Url, Int]
    val links = mutable.ListBuffer.empty[ContentLink]

    logger.debug(s"New run for work ${work.id} ${work.seed} with task root id $id")

    def initialTasks: Set[Task] =
      newTasks(Set(work.seed), 0)

    def newTasks(urls: Set[Url], depth: Int): Set[Task] = {
      urls.groupBy(partition.apply)
        .mapValues(group => group.filter(url => depth < depths.getOrElse(url, Int.MaxValue)))
        .filter { case (_, group) => group.nonEmpty }
        .map { case (part, group) =>
          val taskId = s"$id::${Random.alphanumeric.take(8).mkString}"
          val task = Task(taskId, group, work.criteria, depth, part)
          group.foreach(url => setShallowestUrlDepth(url, depth))
          tasks.put(taskId, task)
          logger.debug(s"New task $taskId for work ${work.id}")
          task
        }
        .toSet
    }

    def release(taskId: String): Set[Task] =
      Set(tasks(taskId))

    def complete(taskId: String, transfer: ContentLinksTransfer): Unit = {
      tasks.remove(taskId)
      transfer.contents.foreach { content =>
        setShallowestUrlDepth(content.url, content.depth)
        links.append(content)
      }
    }

    private def setShallowestUrlDepth(url: Url, depth: Int): Unit = {
      val existingDepthIsSmaller = depths.get(url).exists(_ < depth)
      if (!existingDepthIsSmaller) depths(url) = depth
    }

    def transfers: AllContentLinksTransfer =
      AllContentLinksTransfer(links)

    def isDone: Boolean =
      tasks.isEmpty

    def close(): Unit =
      Run.running.remove(id)

  }

  private object Run {

    private val running = mutable.HashMap.empty[String, Run]

    def createFor(work: Work): Run = {
      val run = new Run(work)
      running.put(run.id, run)
      run
    }

    def forId(taskId: String): Run = {
      val parts = taskId.split("::")
      running(parts.head)
    }

  }

  override def close(): Unit = {}
}

case class AllContentLinksTransfer(all: Seq[ContentLink]) extends ContentLinksTransfer {

  override def contents: Stream[ContentLink] = all.toStream

}