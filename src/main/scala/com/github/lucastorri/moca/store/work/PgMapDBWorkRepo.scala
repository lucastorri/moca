package com.github.lucastorri.moca.store.work

import akka.actor.ActorSystem
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.event.EventBus
import com.github.lucastorri.moca.partition.PartitionSelector
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.serialization.KryoSerialization
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PgMapDBWorkRepo(config: Config, system: ActorSystem, val partition: PartitionSelector, bus: EventBus) extends RunBasedWorkRepo with StrictLogging {

  implicit val exec: ExecutionContext = system.dispatcher

  import PgDriver.api._

  //TODO inject a serializer factory instead of system
  private val ws = new KryoSerialization[CriteriaHolder](system)
  private val ts = new KryoSerialization[ContentLinksTransferHolder](system)

  private val db = Database.forConfig("connection", config)

  private def createWorkTable: DBIO[Int] =
    sqlu"""
          create table if not exists work(
              id varchar(256) not null primary key,
              seed varchar(2048) not null,
              criteria bytea not null)
        """

  private def createRunTable: DBIO[Int] =
    sqlu"""
           create table if not exists run(
              id varchar(128) not null primary key,
              work_id varchar(256) not null,
              criteria bytea not null,
              foreign key(work_id) references work(id))
        """

  private def createLatestResultTable: DBIO[Int] =
    sqlu"""
           create table if not exists latest_result(
              id bigserial primary key,
              work_id varchar(256) not null,
              transfer bytea not null,
              foreign key(work_id) references work(id))
        """

  private def createTaskTable: DBIO[Int] =
    sqlu"""
           create table if not exists task(
              id varchar(256) primary key,
              run_id varchar(128) not null,
              seeds varchar(1024)[] not null,
              depth integer not null,
              partition varchar(256) not null,
              started boolean not null,
              foreign key(run_id) references run(id))
        """

  private def createUrlDepthTable: DBIO[Int] =
    sqlu"""
           create table if not exists url_depth(
              run_id varchar(128) not null,
              url varchar(1024) not null,
              depth integer not null,
              foreign key(run_id) references run(id),
              primary key(run_id, url))
        """

  private def createContentLinkTable: DBIO[Int] =
    sqlu"""
           create table if not exists content_link(
              id bigserial primary key,
              run_id varchar(128) not null,
              transfer bytea not null,
              foreign key(run_id) references run(id))
        """

  private def selectLatestContentLinks(workId: String) =
    sql"""select transfer from latest_result where work_id = $workId order by id desc""".as[(Array[Byte])].headOption
      .map(_.map(ts.deserialize(_).transfer))

  private def insertWork(work: Work): DBIO[Int] = {
    val serializedCriteria = ws.serialize(CriteriaHolder(work.criteria))
    sqlu"""insert into work (id, seed, criteria) values (${work.id}, ${work.seed.toString}, $serializedCriteria)"""
  }

  private def insertRun(id: String, workId: String, criteria: LinkSelectionCriteria): DBIO[Int] = {
    val serializedCriteria = ws.serialize(CriteriaHolder(criteria))
    sqlu"insert into run (id, work_id, criteria) values ($id, $workId, $serializedCriteria)"
  }

  private def selectRunIds() =
    sql"""select id from run""".as[(String)]

  private def selectRun(id: String) = {
    sql"""select id, work_id, criteria from run where id = $id""".as[(String, String, Array[Byte])].headOption.map {
      case Some((runId, workId, criteria)) => Some((runId, workId, ws.deserialize(criteria).criteria))
      case None => None
    }
  }

  private def insertTask(runId: String, task: Task): DBIO[Int] =
    sqlu"""insert into task (id, run_id, seeds, depth, partition, started)
          values (${task.id}, $runId, ${task.seeds.map(_.toString).toSeq}, ${task.initialDepth}, ${task.partition}, false)"""

  private def insertUrlDepth(runId: String, url: Url, depth: Int): DBIO[Int] =
    sqlu"""insert into url_depth (run_id, url, depth) values ($runId, ${url.toString}, $depth)"""

  private def updateUrlDepth(runId: String, url: Url, depth: Int): DBIO[Int] =
    sqlu"""update url_depth set depth = $depth where run_id = $runId and url = ${url.toString}"""

  private def insertContentLink(runId: String, transfer: ContentLinksTransfer): DBIO[Int] =
    sqlu"""insert into content_link (run_id, transfer) values ($runId, ${ts.serialize(ContentLinksTransferHolder(transfer))})"""

  private def updateTaskNotStarted(taskId: String): DBIO[Int] =
    sqlu"""update task set started = false where id = $taskId"""

  private def deleteFromUrlDepth(runId: String): DBIO[Int] =
    sqlu"""delete from url_depth where run_id = $runId"""

  private def deleteFromTask(taskId: String): DBIO[Int] =
    sqlu"""delete from task where id = $taskId"""

  private def selectFromTaskWhereOpen() =
    sql"""update task set started = true where not started returning id, run_id, seeds, depth, partition""".as[(String, String, Seq[String], Int, String)]

  private def deleteFromRun(runId: String): DBIO[Int] =
    sqlu"""delete from run where id = $runId"""

  private def deleteFromContentLink(runId: String): DBIO[Int] =
    sqlu"""delete from content_link where run_id = $runId"""

  private def selectTransfersFromContentLink(runId: String) =
    sql"""select transfer from content_link where run_id = $runId""".as[(Array[Byte])].map { transfers =>
      transfers.map(ts.deserialize(_).transfer)
    }

  private def insertIntoLatestResult(workId: String, transfer: ContentLinksTransfer): DBIO[Int] =
    sqlu"""insert into latest_result (work_id, transfer) values ($workId, ${ts.serialize(ContentLinksTransferHolder(transfer))})"""

  private def selectUrlsFromUrlDepth(runId: String) =
    sql"""select url, depth from url_depth where run_id = $runId""".as[(String, Int)]

  private def selectIdsFromTask(runId: String) =
    sql"""select id from task where run_id = $runId""".as[(String)]


  override protected def loadRun(runId: String): Future[Run] = safe {
    db.run(selectRun(runId)).flatMap {
      case Some((id, workId, criteria)) =>
        val urlsResult = db.run(selectUrlsFromUrlDepth(runId))
        val tasksResult = db.run(selectIdsFromTask(runId))
        for {
          urls <- urlsResult
          tasks <- tasksResult
        } yield {
          val depths = urls.map { case (url, depth) => Url(url).hashCode -> depth }.toMap
          val run = Run(id, workId, criteria)
          run.depths.putAll(depths)
          run.allTasks.addAll(tasks)
          run
        }
      case None =>
        Future.failed(new RuntimeException(s"Run $runId not found"))
    }
  }

  override protected def saveRunAndWork(run: Run, work: Work): Future[Unit] = safe {
    val action = DBIO.seq(
      insertWork(work),
      insertRun(run.id, run.workId, run.criteria)
    )
    db.run(action.transactionally).map(_ => ())
  }

  override protected def saveTaskDone(run: Run, taskId: String, transfer: ContentLinksTransfer, last: Boolean): Future[Unit] = safe {

    if (last) {
      db.run(selectTransfersFromContentLink(run.id))
        .map(transfers => CombinedLinksTransfer(transfers :+ transfer))
        .flatMap { combined =>
          db.run(DBIO.seq(
            deleteFromTask(taskId),
            insertIntoLatestResult(run.workId, combined),
            deleteFromContentLink(run.id),
            deleteFromUrlDepth(run.id),
            deleteFromRun(run.id)
          ).transactionally).map(_ => ())
        }
    } else {
      val depthUpdates = transfer.contents.map { link =>
        if (run.compareDepth(link.url, link.depth).isNew) insertUrlDepth(run.id, link.url, link.depth)
        else updateUrlDepth(run.id, link.url, link.depth)
      }
      db.run(DBIO.sequence(
        Seq(insertContentLink(run.id, transfer), deleteFromTask(taskId)) ++ depthUpdates
      ).transactionally).map(_ => ())
    }
  }

  override protected def saveTasks(run: Run, tasks: Set[Task]): Future[Unit] = safe {
    val addTasks = tasks.toSeq.map(insertTask(run.id, _))
    val updateDepths =
      for {
        task <- tasks
        url <- task.seeds
      } yield {
        if (run.compareDepth(url, task.initialDepth).isNew) insertUrlDepth(run.id, url, task.initialDepth)
        else updateUrlDepth(run.id, url, task.initialDepth)
      }

    db.run(DBIO.sequence(addTasks ++ updateDepths).transactionally).map(_ => publish())
  }

  override protected def saveRelease(run: Run, taskIds: Set[String]): Future[Unit] = safe {
    db.run(DBIO.sequence(taskIds.toSeq.map(updateTaskNotStarted)).transactionally).map(_ => publish())
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = safe {
    db.run(selectLatestContentLinks(workId))
  }

  override def close(): Unit = {
    db.close()
  }

  private def publish(): Unit = {
    db.run(selectFromTaskWhereOpen()).onComplete {
      case Success(tasks) =>
        tasks.groupBy { case (_, runId, _, _, _) => runId }.foreach { case (runId, byRunId) =>
          RunControl.get(runId).foreach { run =>
            byRunId.foreach { case (taskId, _, seeds, depth, part) =>
              bus.publish(EventBus.NewTasks, Task(taskId, seeds.map(Url.apply).toSet, run.criteria, depth, part))
            }
          }
        }
      case Failure(t) =>
        logger.error("Could not load tasks ready", t)
    }
  }

  override protected def init(): Future[Unit] = safe {
    val create = for {
      _ <- createWorkTable
      _ <- createRunTable
      _ <- createLatestResultTable
      _ <- createTaskTable
      _ <- createUrlDepthTable
      _ <- createContentLinkTable
    } yield ()

    db.run(create.transactionally)
  }

  override protected def listRuns(): Future[Set[String]] = safe {
    db.run(selectRunIds()).map(_.toSet)
  }

  private def safe[T](f: => Future[T]): Future[T] =
    try f
    catch { case e: Exception => Future.failed(e) }

  start()

}

case class CombinedLinksTransfer(transfers: Seq[ContentLinksTransfer]) extends ContentLinksTransfer {
  override def contents: Stream[ContentLink] = {
    transfers.foldLeft(Stream.empty[ContentLink]) { case (stream, next) =>
      stream #::: next.contents
    }
  }
}

case class CriteriaHolder(criteria: LinkSelectionCriteria)
case class ContentLinksTransferHolder(transfer: ContentLinksTransfer)