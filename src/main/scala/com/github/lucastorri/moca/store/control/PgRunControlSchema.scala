package com.github.lucastorri.moca.store.control

import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.url.Url

trait PgRunControlSchema { self: PgRunControl =>

  import com.github.lucastorri.moca.store.control.PgDriver.api._


  protected def createWorkTable: DBIO[Int] =
    sqlu"""
           create table if not exists work(
              id varchar(256) not null primary key,
              seed varchar(2048) not null,
              criteria bytea not null)
        """

  protected def insertWork(work: Work) = {
    val serializedCriteria = ws.serialize(CriteriaHolder(work.criteria))
    sqlu"insert into work (id, seed, criteria) values (${work.id}, ${work.seed.toString}, $serializedCriteria)"
  }


  protected def createUrlDepthTable: DBIO[Int] =
    sqlu"""
           create table if not exists url_depth(
              run_id varchar(128) not null,
              url varchar(1024) not null,
              depth integer not null,
              foreign key(run_id) references run(id),
              primary key(run_id, url))
        """

  protected def insertUrlDepth(runId: String, url: Url, depth: Int) = {
    sqlu"insert into url_depth (run_id, url, depth) values ($runId, ${url.toString}, $depth)"
  }

  protected def updateUrlDepth(runId: String, url: Url, depth: Int) = {
    sqlu"update url_depth set depth = $depth where run_id = $runId and url = ${url.toString}"
  }

  protected def deleteUrlDepths(runId: String) = {
    sqlu"delete from url_depth where run_id = $runId"
  }

  protected def selectUrlDepths(runId: String) = {
    sql"select url, depth from url_depth where run_id = $runId".as[(String, Int)]
      .map(_.map { case (url, depth) => Url(url) -> depth })
  }


  protected def createRunTable: DBIO[Int] =
    sqlu"""
           create table if not exists run(
              id varchar(128) not null primary key,
              work_id varchar(256) not null,
              criteria bytea not null,
              foreign key(work_id) references work(id))
        """

  protected def insertRun(run: Run) = {
    val serializedCriteria = ws.serialize(CriteriaHolder(run.criteria))
    sqlu"insert into run (id, work_id, criteria) values (${run.id}, ${run.workId}, $serializedCriteria)"
  }

  protected def deleteRun(runId: String) = {
    sqlu"delete from run where id = $runId"
  }

  protected def selectAllRuns = {
    sql"select id, work_id, criteria from run".as[(String, String, Array[Byte])].map { results =>
      results.map { case (id, workId, serializedCriteria) =>
        Run(id, workId, ws.deserialize(serializedCriteria).criteria)
      }
    }
  }


  protected def createOutstandingUrlTable: DBIO[Int] =
    sqlu"""
           create table if not exists outstanding_url(
              run_id varchar(128) not null,
              url varchar(1024) not null,
              depth integer not null,
              partition varchar(256) not null,
              foreign key(run_id) references run(id),
              primary key(run_id, url))
        """

  protected def insertOutstandingUrl(runId: String, url: Url, depth: Int, partition: String) = {
    sqlu"insert into outstanding_url(run_id, url, depth, partition) values ($runId, ${url.toString}, $depth, $partition)"
  }

  protected def deleteOutstandingUrl(runId: String, url: Url, depth: Int, partition: String) = {
    sqlu"delete from outstanding_url where run_id = $runId and url = ${url.toString} and depth = $depth and partition = $partition"
  }

  protected def deleteOutstandingUrl(runId: String) = {
    sqlu"delete from outstanding_url where run_id = $runId"
  }

  protected def selectOutstandingUrls(runId: String, partition: String) = {
    sql"select url, depth from outstanding_url where run_id = $runId and partition = $partition".as[(String, Int)]
      .map(_.map { case (url, depth) => Url(url) -> depth })
  }


  protected def createFinalResultTable =
    sqlu"""
           create table if not exists final_result(
              id bigserial primary key,
              work_id varchar(256) not null,
              transfer bytea not null,
              foreign key(work_id) references work(id))
        """

  protected def insertFinalResult(workId: String, transfer: ContentLinksTransfer) = {
    val serializedTransfer = ts.serialize(ContentLinksTransferHolder(transfer))
    sqlu"insert into final_result (work_id, transfer) values ($workId, $serializedTransfer)"
  }

  protected def selectFinalResult(workId: String) = {
    sql"select transfer from final_result where work_id = $workId order by id desc".as[Array[Byte]]
      .headOption
      .map(_.map(bytes => ts.deserialize(bytes).transfer))
  }


  protected def createPartialResultTable =
    sqlu"""
           create table if not exists partial_result(
              id bigserial primary key,
              run_id varchar(128) not null,
              transfer bytea not null,
              foreign key(run_id) references run(id))
        """

  protected def insertPartialResult(runId: String, transfer: ContentLinksTransfer) = {
    val serializedTransfer = ts.serialize(ContentLinksTransferHolder(transfer))
    sqlu"insert into partial_result (run_id, transfer) values ($runId, $serializedTransfer)"
  }

  protected def deletePartialResults(runId: String) = {
    sqlu"delete from partial_result where run_id = $runId"
  }

  protected def selectPartialResults(runId: String) = {
    sql"select transfer from partial_result where run_id = $runId order by id asc".as[Array[Byte]]
      .map(_.map(bytes => ts.deserialize(bytes).transfer))
  }


  protected def createTaskTable: DBIO[Int] =
    sqlu"""
           create table if not exists task(
              id varchar(256) primary key,
              run_id varchar(128) not null,
              seeds varchar(1024)[] not null,
              depth integer not null,
              partition varchar(256) not null,
              foreign key(run_id) references run(id))
        """

  protected def insertTask(runId: String, task: Task) = {
    val seeds = task.seeds.map(_.toString).toSeq
    sqlu"insert into task (id, run_id, seeds, depth, partition) values (${task.id}, $runId, $seeds, ${task.initialDepth}, ${task.partition})"
  }

  protected def selectTaskIdsAndPartitions(runId: String) = {
    sql"select id, partition from task where run_id = $runId".as[(String, String)]
  }

  protected def selectTask(taskId: String, criteria: LinkSelectionCriteria) = {
    sql"select id, run_id, seeds, depth, partition from task where id = $taskId".as[(String, String, Seq[String], Int, String)]
      .headOption
      .map(_.map { case (id, runId, seeds, depth, part) => Task(id, seeds.toSet.map(Url.apply), criteria, depth, part) })
  }

  protected def deleteTask(runId: String, taskId: String) = {
    sqlu"delete from task where id = $taskId and run_id = $runId"
  }

  protected def deleteTasks(runId: String) = {
    sqlu"delete from task where run_id = $runId"
  }

}