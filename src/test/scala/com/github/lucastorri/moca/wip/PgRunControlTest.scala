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
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example1.com/"), FakeCriteria),
        Work("2", Url("http://www.example2.com/"), FakeCriteria))))

      published must have size 2

      val expectedSeeds = published.flatMap(_.seeds).map(_.toString).sorted
      expectedSeeds must equal(Seq("http://www.example1.com/", "http://www.example2.com/"))

    }
  }

  it must "not add currently running work" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example.com"), FakeCriteria),
        Work("2", Url("http://www.example.com"), FakeCriteria))))

      result(control.add(Set(
        Work("1", Url("http://www.example.com"), FakeCriteria),
        Work("2", Url("http://www.example.com"), FakeCriteria))))

      published must have size 2

    }
  }

  it must "add a new task if partition is not busy" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example.com/"), FakeCriteria))))

      val task = published.head

      result(control.subTasks(task.id, 1, Set(Url("http://www.example1.com/"))))

      published must have size 2
      published.last.seeds must equal (Set(Url("http://www.example1.com/")))

    }
  }

  it must "not schedule a task if the partition is busy" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example.com/"), FakeCriteria))))

      val task = published.head

      result(control.subTasks(task.id, 1, Set(Url("http://www.example.com/other-path"))))

      published must have size 1

    }
  }

  it must "complain when completing a non existing task" in new context {
    withControl { control =>

      val r = control.done("blah", FakeTransfer())

      an[Exception] must be thrownBy { result(r) }

    }
  }

  it must "complete a non final task" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example1.com"), FakeCriteria))))

      val task = published.head

      result(control.subTasks(task.id, 1, Set(Url("http://www.example2.com"))))

      result(control.done(task.id, FakeTransfer())) must be (None)
      result(control.links("1")) must be (None)

    }
  }

  it must "not schedule an awaiting busy partition if results have a shallower depth" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example.com"), FakeCriteria))))

      val task = published.head

      val url = Url("http://www.example.com/another-url")
      result(control.subTasks(task.id, 1, Set(url)))
      published must have size 1

      result(control.done(task.id, FakeTransfer(ContentLink(url, "", 1, "")))) must be (Some("1"))

    }
  }

  it must "schedule an awaiting busy partition" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example.com"), FakeCriteria))))

      val task = published.head

      val url = Url("http://www.example.com/another-url")
      result(control.subTasks(task.id, 1, Set(url)))
      published must have size 1

      result(control.done(task.id, FakeTransfer(ContentLink(url, "", 2, "")))) must be (None)

      published must have size 2
      published.last.seeds must equal (Set(url))

      result(control.done(published.last.id, FakeTransfer())) must be (Some("1"))

    }
  }

  it must "return fetched content for work" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("1", Url("http://www.example1.com"), FakeCriteria),
        Work("2", Url("http://www.example2.com"), FakeCriteria))))

      val task1 = published.head
      val task2 = published.last

      result(control.subTasks(task1.id, 1, Set(Url("http://www.example3.com/"))))
      result(control.subTasks(task2.id, 1, Set(Url("http://www.example3.com/"))))
      result(control.subTasks(task1.id, 2, Set(Url("http://www.example1.com/test"))))

      published.foreach { t =>
        val links = t.seeds.map(url => ContentLink(url, "", t.initialDepth, "")).toSeq
        result(control.done(t.id, FakeTransfer(links: _*)))
      }

      result(control.links("1")).get.contents.toSet must equal (Set(
        ContentLink(Url("http://www.example1.com"), "", 0, ""),
        ContentLink(Url("http://www.example3.com"), "", 1, ""),
        ContentLink(Url("http://www.example1.com/test"), "", 2, "")))

      result(control.links("2")).get.contents.toSet must equal (Set(
        ContentLink(Url("http://www.example2.com"), "", 0, ""),
        ContentLink(Url("http://www.example3.com"), "", 1, "")))

    }
  }

  it must "republish a task if it was aborted" in new context {
    withControl { control =>

      result(control.add(Set(
        Work("7", Url("http://www.example.com/"), FakeCriteria))))

      val task = published.head

      result(control.abort(task.id))

      published must have size 2
      published.head must equal (published.last)

      result(control.done(task.id, FakeTransfer())) must equal (Some("7"))

    }
  }

  it must "continue after restart" in new context {
    withControl { control =>

      result(control.add(Set(Work("3", Url("http://www.example1.com/"), FakeCriteria))))
      
      result(control.subTasks(published.head.id, 1, Set(Url("http://www.example2.com/"))))

      result(control.subTasks(published.last.id, 2, Set(Url("http://www.example1.com/other-url"))))
      
    }
    withControl { control =>

      result(control.done(published.head.id,
        FakeTransfer(ContentLink(Url("http://www.example1.com/other-url"), "", 3, ""))))

      val last = published.last
      (last.seeds -> last.initialDepth) must equal (Set(Url("http://www.example1.com/other-url")) -> 2)

      val done = published.tail.map { t =>
        result(control.done(t.id, FakeTransfer()))
      }

      done must equal (Seq(None, Some("3")))

    }
  }

  trait context {

    val published = mutable.ListBuffer.empty[Task]

    val publisher = new TaskPublisher {
      override def push(tasks: Set[Task]): Future[Unit] = {
        published ++= tasks
        Future.successful(())
      }
    }
    
    def withControl(f: PgRunControl => Unit): Unit = {
      val control = new PgRunControl(config, new KryoSerializerService(system), publisher, new ByHostPartitionSelector, system.dispatcher)
      f(control)
      control.close()
    }

  }

  def result[T](f: Future[T]): T = Await.result(f, 5.seconds)

}

case class FakeTransfer(items: ContentLink*) extends ContentLinksTransfer {
  override def contents: Stream[ContentLink] = items.toStream
}

case object FakeCriteria extends LinkSelectionCriteria {
  override def select(task: Task, link: Link, page: RenderedPage): Set[Url] = Set.empty
}