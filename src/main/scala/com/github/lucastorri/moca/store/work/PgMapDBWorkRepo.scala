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
              url varchar(1024) not null,
              uri varchar(1024) not null,
              depth integer not null,
              hash varchar(256) not null,
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
              url varchar(1024) not null,
              uri varchar(1024) not null,
              depth integer not null,
              hash varchar(256) not null,
              foreign key(run_id) references run(id))
        """

  private def selectLatestContentLinks(workId: String) =
    sql"""select url, uri, depth, hash from createLatestResultTable where work_id = $workId""".as[(String, String, Int, String)]

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

  private def insertContentLink(runId: String, link: ContentLink): DBIO[Int] =
    sqlu"""insert into content_link (run_id, url, uri, depth, hash) values ($runId, ${link.url.toString}, ${link.uri}, ${link.depth}, ${link.hash})"""

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

  private def deleteFromLatestResult(workId: String): DBIO[Int] =
    sqlu"""delete from latest_result where work_id = $workId"""

  private def deleteFromContentLink(runId: String): DBIO[Int] =
    sqlu"""delete from content_link where run_id = $runId"""

  private def moveFromContentLinkToLatestRun(workId: String, runId: String): DBIO[Int] =
    sqlu"""insert into latest_result (work_id, url, uri, depth, hash) select $workId, url, uri, depth, hash from content_link where run_id = $runId"""

  private def selectUrlsFromUrlDepth(runId: String) =
    sql"""select url, depth from url_depth where run_id = $runId""".as[(String, Int)]

  private def selectIdsFromTask(runId: String) =
    sql"""select id from task where run_id = $runId""".as[(String)]

  override protected def loadRun(runId: String): Future[Run] = {
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

  override protected def saveRunAndWork(run: Run, work: Work): Future[Unit] = {
    val action = DBIO.seq(
      insertWork(work),
      insertRun(run.id, run.workId, run.criteria)
    )
    db.run(action.transactionally).map(_ => ())
  }

  override protected def saveTaskDone(run: Run, taskId: String, transfer: ContentLinksTransfer, last: Boolean): Future[Unit] = {

    val addContents = transfer.contents.map(insertContentLink(run.id, _)) :+ deleteFromTask(taskId)

    val allActions =
    if (last) {
      addContents ++ Seq(
        deleteFromLatestResult(run.workId),
        moveFromContentLinkToLatestRun(run.workId, run.id),
        deleteFromContentLink(run.id),
        deleteFromUrlDepth(run.id),
        deleteFromRun(run.id))
    } else {
      addContents
    }

    db.run(DBIO.sequence(allActions).transactionally).map(_ => ())
  }

  override protected def saveTasks(run: Run, tasks: Set[Task], newUrls: Set[Url], shallowerUrls: Set[Url]): Future[Unit] = {
    val addTasks = tasks.toSeq.map(insertTask(run.id, _))
    val updateDepths =
      for {
        task <- tasks
        url <- task.seeds
      } yield {
        if (newUrls.contains(url)) insertUrlDepth(run.id, url, task.initialDepth)
        else updateUrlDepth(run.id, url, task.initialDepth)
      }

    db.run(DBIO.sequence(addTasks ++ updateDepths).transactionally).map(_ => publish())
  }

  override protected def saveRelease(run: Run, taskIds: Set[String]): Future[Unit] = {
    db.run(DBIO.sequence(taskIds.toSeq.map(updateTaskNotStarted)).transactionally).map(_ => publish())
  }

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = {
    db.run(selectLatestContentLinks(workId)).map {
      case v if v.isEmpty => None
      case v =>
        val links = v.map { case (url, uri, depth, hash) => ContentLink(Url(url), uri, depth, hash) }
        Some(AllContentLinksTransfer(links))
    }
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

  override protected def init(): Future[Unit] = {
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

  override protected def listRuns(): Future[Set[String]] =
    db.run(selectRunIds()).map(_.toSet)

  start()
}

case class CriteriaHolder(criteria: LinkSelectionCriteria)