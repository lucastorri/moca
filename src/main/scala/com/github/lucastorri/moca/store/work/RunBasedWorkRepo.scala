package com.github.lucastorri.moca.store.work

import java.util.concurrent.Semaphore

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Await._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Random, Try}


//TODO make this a regular class, that works tightly with Master, and receives a WorkRepo
trait RunBasedWorkRepo extends WorkRepo {

  implicit def exec: ExecutionContext

  def partition: PartitionSelector

  protected def init(): Future[Unit]

  protected def listRuns(): Future[Set[String]]

  protected def loadRun(runId: String): Future[Run]

  protected def saveRunAndWork(run: Run, work: Work): Future[Unit]

  protected def saveTasks(run: Run, tasks: Set[Task]): Future[Unit]

  protected def saveTaskDone(run: Run, taskId: String, transfer: ContentLinksTransfer, last: Boolean): Future[Unit]

  protected def saveRelease(run: Run, taskIds: Set[String]): Future[Unit]


  protected def start(): Unit = {
    result(init(), 15.seconds)

    result({
      listRuns().flatMap { runs =>
        val loadingAll = runs.toSeq.map(runId => RunControl.get(runId))
        Future.sequence(loadingAll)
      }
    }, 60.seconds)
  }


  object RunControl {

    private val running = new java.util.concurrent.ConcurrentHashMap[String, Run]
    private val creating = new java.util.concurrent.ConcurrentHashMap[String, Future[Run]]
    private val loading = new java.util.concurrent.ConcurrentHashMap[String, Future[Run]]

    private def init(run: Run, work: Work): Future[Unit] = {
      run.subTasks(0, Set(work.seed))
    }

    def create(work: Work): Future[Run] = {
      Option(creating.get(work.id)) match {
        case Some(f) => f
        case None =>
          val promise = Promise[Run]()
          creating.put(work.id, promise.future)
          val run = Run(newRunId, work.id, work.criteria)
          promise.completeWith {
            for {
              _ <- saveRunAndWork(run, work)
              _ <- init(run, work)
            } yield {
              running.put(run.id, run)
              run
            }
          }

          promise.future.onComplete { case _ => creating.remove(work.id) }
          promise.future
      }
    }

    def get(id: String): Future[Run] = {
      val runId = idFor(id)
      Option(running.get(runId)) match {
        case Some(run) => Future.successful(run)
        case None =>
          Option(loading.get(runId)) match {
            case Some(loaded) => loaded
            case None =>
              val promise = Promise[Run]()
              loading.put(runId, promise.future)
              promise.completeWith {
                for {
                  run <- loadRun(runId)
                } yield {
                  running.put(runId, run)
                  run
                }
              }

              promise.future.onComplete { case _ => loading.remove(runId) }
              promise.future
          }

      }
    }
    
    def close(runId: String): Unit = {
      running.remove(runId)
    }

    val separator = "::"

    def idFor(id: String): String = id.split(separator).head

    def newRunId = Random.alphanumeric.take(32).mkString

    def newTaskId(runId: String) = s"$runId$separator${Random.alphanumeric.take(16).mkString}"

  }

  case class Run(id: String, workId: String, criteria: LinkSelectionCriteria) {

    val lock = new Semaphore(1, true)
    //TODO use mapdb on a temp file
    val allTasks = mutable.HashSet.empty[String]
    val depths = mutable.HashMap.empty[Int, Int]

    def subTasks(depth: Int, links: Set[Url]): Future[Unit] = locked {
      val newUrls = links.filter(compareDepth(_, depth).shouldAdd)
      val tasks = newUrls.groupBy(partition.apply).map { case (part, urls) =>
        Task(RunControl.newTaskId(id), urls, criteria, depth, part)
      }.toSet
      saveTasks(this, tasks).map { _ =>
        allTasks.addAll(tasks.map(_.id))
        newUrls.foreach(url => depths.put(url.hashCode, depth))
      }
    }

    def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = locked {
      val last = isLast(taskId)
      saveTaskDone(this, taskId, transfer, last).map { _ =>
        allTasks.remove(taskId)
        if (last) {
          RunControl.close(id)
          Some(workId)
        } else {
          None
        }
      }
    }

    def compareDepth(url: Url, newDepth: Int): DepthState = {
      depths.get(url.hashCode) match {
        case Some(depth) => if (newDepth < depth) SmallerThanExisting else ExistingIsSmaller
        case None => NewDepth
      }
    }

    def release(taskIds: Set[String]): Future[Unit] = locked {
      saveRelease(this, taskIds).map { _ => () }
    }

    def isLast(taskId: String): Boolean = {
      allTasks.forall(_ == taskId)
    }

    def locked[T](action: => Future[T]): Future[T] = {
      try {
        lock.acquire()
        val f = action
        f.onComplete { case _ => lock.release() }
        f
      } catch { case e: Exception =>
        Try(lock.release())
        Future.failed(e)
      }
    }

  }

  override def addWork(added: Set[Work]): Future[Boolean] =
    Future.sequence(added.map(RunControl.create)).map(_ => true)

  override def addTask(parentTaskId: String, linksDepth: Int, links: Set[Url]): Future[Unit] = {
    for {
      run <- RunControl.get(parentTaskId)
      _ <- run.subTasks(linksDepth, links)
    } yield ()
  }

  override def release(taskId: String): Future[Unit] = releaseAll(Set(taskId))

  override def releaseAll(taskIds: Set[String]): Future[Unit] = {
    Future.sequence {
      taskIds.groupBy(RunControl.idFor).map { case (runId, ids) =>
        for {
          run <- RunControl.get(runId)
          _ <- run.release(ids)
        } yield ()
      }
    }.map(_ => ())
  }

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] =
    RunControl.get(taskId).flatMap(_.done(taskId, transfer))

  sealed abstract class DepthState(val shouldAdd: Boolean, val isNew: Boolean)
  case object NewDepth extends DepthState(true, true)
  case object SmallerThanExisting extends DepthState(true, false)
  case object ExistingIsSmaller extends DepthState(false, false)

}
