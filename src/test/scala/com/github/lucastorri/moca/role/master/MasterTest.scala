package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.pattern.ask
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.{RoleTest, Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.store.control.{EmptyCriteria, FixedTransfer, RunControl}
import com.github.lucastorri.moca.url.Url
import org.scalatest.{FlatSpec, MustMatchers}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

class MasterTest extends FlatSpec with MustMatchers with RoleTest {

  override val port = 9898
  override val config = """
     |moca.master.journal-plugin-id = "journal"
     |moca.master.snapshot-plugin-id = "snapshot"
     |
     |journal {
     |  class = "com.github.lucastorri.moca.role.EmptyJournal"
     |  plugin-dispatcher = "akka.actor.default-dispatcher"
     |}
     |
     |snapshot {
     |  class = "com.github.lucastorri.moca.role.EmptySnapshotStore"
     |  plugin-dispatcher = "akka.actor.default-dispatcher"
     |}
   """.stripMargin


  it must "close control before dying" in new context {
    withMaster { master =>

      master ! PoisonPill
      result(control.closed) must be (right = true)

    }
  }

  it must "add new work" in new context {
    withMaster { master =>

      val newWork = Set(Work("1", Url("http://www.example.com"), EmptyCriteria))

      master ! AddWork(newWork)
      master ! PoisonPill

      result(control.closed)

      control.workAdded.head must equal (newWork)

    }
  }

  it must "ack when a task is finished" in new context {
    withMaster { master =>

      result(master ? TaskFinished(null, "1", FixedTransfer())) must be (Ack)

    }
  }

  it must "not offer two tasks with the same partition" in new context {
    withMaster { master =>

      val task1 = Task("1", Set(Url("http://www.example.com")), EmptyCriteria, 0, "a")
      val task2 = Task("2", Set(Url("http://www.example.com")), EmptyCriteria, 0, "a")
      control.publish(Set(task1))
      control.publish(Set(task2))

      val (m1, reply1) = requestTask(master)
      reply1 must equal (task1)

      val (m2, reply2) = requestTask(master)
      reply2 must be (Nack)

      master ! TaskFinished(m1.ref, "1", FixedTransfer())

      val (m3, reply3) = requestTask(master)
      reply3 must equal (task2)

      control.taskDone must equal (Seq("1"))

      Seq(m1, m2, m3).foreach(_.close())

    }
  }


  override protected def afterAll(): Unit = {
    system.terminate()
  }

  trait context {

    val control = MemRunControl()
    val sleep = 50

    def withMaster(f: ActorRef => Unit): Unit = {
      val master = system.actorOf(Props(new Master(control)))
      while (!control.hasSubscriber) Thread.sleep(sleep)
      f(master)
      system.stop(master)
    }

    def requestTask(master: ActorRef, max: Int = 3): (Messenger, Any) = {
      val m = Messenger()
      master.tell(TaskRequest(m.ref), m.ref)
      val returned = result(m.result[Any])
      returned match {
        case TaskOffer(task) =>
          m -> task
        case _ if max == 0 =>
          m -> returned
        case _ =>
          m.close()
          Thread.sleep(sleep)
          requestTask(master, max-1)
      }
    }

  }

}

case class Messenger(ack: Boolean = true)(implicit system: ActorSystem) {

  val ref = system.actorOf(Props(new A))
  private val promise = Promise[Any]()

  def result[T : ClassTag]: Future[T] = promise.future.mapTo[T]

  def close(): Unit = system.stop(ref)

  class A extends Actor {
    override def receive: Receive = {
      case v =>
        promise.success(v)
        if (ack) sender() ! Ack
    }
  }

}

case class MemRunControl() extends RunControl {

  private val _closed = Promise[Boolean]()

  def closed: Future[Boolean] = _closed.future

  val workAdded = mutable.ListBuffer.empty[Set[Work]]

  val taskDone = mutable.ListBuffer.empty[String]

  override def add(works: Set[Work]): Future[Unit] = {
    workAdded += works
    Future.successful(())
  }

  override def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit] = sys.error("Not implemented")

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = sys.error("Not implemented")

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = {
    taskDone.append(taskId)
    Future.successful(None)
  }


  override def abort(taskIds: Set[String]): Future[Unit] = sys.error("Not implemented")

  override def close(): Unit =
    _closed.success(true)

}