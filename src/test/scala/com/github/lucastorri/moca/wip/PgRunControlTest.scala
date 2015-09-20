package com.github.lucastorri.moca.wip

import akka.actor.ActorSystem
import com.github.lucastorri.moca.browser.RenderedPage
import com.github.lucastorri.moca.criteria.LinkSelectionCriteria
import com.github.lucastorri.moca.partition.ByHostPartitionSelector
import com.github.lucastorri.moca.role.{Work, Task}
import com.github.lucastorri.moca.role.worker.Link
import com.github.lucastorri.moca.store.content.{ContentLink, ContentLinksTransfer}
import com.github.lucastorri.moca.store.serialization.KryoSerializerService
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterEach, MustMatchers, FlatSpec}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class PgRunControlTest extends FlatSpec with MustMatchers with BeforeAndAfterEach {

  import com.github.lucastorri.moca.store.work.PgDriver.api._

  val system = ActorSystem()

  val config = ConfigFactory.parseString(
    """class = "com.github.lucastorri.moca.wip.PgRunControl"
      |  connection {
      |  dataSourceClass = "org.postgresql.ds.PGSimpleDataSource"
      |  properties {
      |    databaseName = "moca"
      |  }
      |}
      |init-timeout = 30s
    """.stripMargin)

  val db = Database.forConfig("connection", config)

  override protected def beforeEach(): Unit = {
    result(db.run(DBIO.seq(sqlu"drop schema public cascade", sqlu"create schema public")))
  }

  it must "add work" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example1.com/"), FakeCriteria),
      Work("2", Url("http://www.example2.com/"), FakeCriteria))))

    publisher.buffer must have size 2

    val expectedSeeds = publisher.buffer.flatMap(_.seeds).map(_.toString).sorted
    expectedSeeds must equal (Seq("http://www.example1.com/", "http://www.example2.com/"))

    control.close()
  }

  it must "not add currently running work" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example.com"), FakeCriteria),
      Work("2", Url("http://www.example.com"), FakeCriteria))))

    result(control.add(Set(
      Work("1", Url("http://www.example.com"), FakeCriteria),
      Work("2", Url("http://www.example.com"), FakeCriteria))))

    publisher.buffer must have size 2

    control.close()
  }

  it must "add a new task if partition is not busy" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example.com/"), FakeCriteria))))

    val task = publisher.buffer.head

    result(control.subTasks(task.id, 1, Set(Url("http://www.example1.com/"))))

    publisher.buffer must have size 2
    publisher.buffer.last.seeds must equal (Set(Url("http://www.example1.com/")))

    control.close()
  }

  it must "not schedule a task if the partition is busy" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example.com/"), FakeCriteria))))

    val task = publisher.buffer.head

    result(control.subTasks(task.id, 1, Set(Url("http://www.example.com/other-path"))))

    publisher.buffer must have size 1

    control.close()
  }

  it must "complain when completing a non existing task" in new context {

    val r = control.done("blah", FakeTransfer())

    an[Exception] must be thrownBy { result(r) }

    control.close()
  }

  it must "complete a non final task" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example1.com"), FakeCriteria))))

    val task = publisher.buffer.head

    result(control.subTasks(task.id, 1, Set(Url("http://www.example2.com"))))

    result(control.done(task.id, FakeTransfer())) must be (None)
    result(control.links("1")) must be (None)

    control.close()
  }

  it must "not schedule an awaiting busy partition if results have a shallower depth" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example.com"), FakeCriteria))))

    val task = publisher.buffer.head

    val url = Url("http://www.example.com/another-url")
    result(control.subTasks(task.id, 1, Set(url)))
    publisher.buffer must have size 1

    result(control.done(task.id, FakeTransfer(ContentLink(url, "", 1, "")))) must be (Some("1"))


    control.close()
  }

  it must "schedule an awaiting busy partition" in new context {

    result(control.add(Set(
      Work("1", Url("http://www.example.com"), FakeCriteria))))

    val task = publisher.buffer.head

    val url = Url("http://www.example.com/another-url")
    result(control.subTasks(task.id, 1, Set(url)))
    publisher.buffer must have size 1

    result(control.done(task.id, FakeTransfer(ContentLink(url, "", 2, "")))) must be (None)

    publisher.buffer must have size 2
    publisher.buffer.last.seeds must equal (Set(url))

    result(control.done(publisher.buffer.last.id, FakeTransfer())) must be (Some("1"))

    control.close()
  }

  it must "return fetched content for work" in new context {
    //TODO
  }

  it must "republish a task if it was aborted" in new context {

    result(control.add(Set(
      Work("7", Url("http://www.example.com/"), FakeCriteria))))

    val task = publisher.buffer.head

    result(control.abort(task.id))

    publisher.buffer must have size 2
    publisher.buffer.head must equal (publisher.buffer.last)

    result(control.done(task.id, FakeTransfer())) must equal (Some("7"))

    control.close()
  }

  //TODO have two awaiting partitions

  //TODO test restart

  trait context {

    val publisher = new TaskPublisher {
      val buffer = mutable.ListBuffer.empty[Task]
      override def push(tasks: Set[Task]): Future[Unit] = {
        buffer ++= tasks
        Future.successful(())
      }
    }

    val control = new PgRunControl(config, new KryoSerializerService(system), publisher, new ByHostPartitionSelector, system.dispatcher)

  }


  def result[T](f: Future[T]): T = Await.result(f, 5.seconds)

}

case class FakeTransfer(items: ContentLink*) extends ContentLinksTransfer {
  override def contents: Stream[ContentLink] = items.toStream
}

case object FakeCriteria extends LinkSelectionCriteria {
  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = Set.empty
}