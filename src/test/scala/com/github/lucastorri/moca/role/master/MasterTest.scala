package com.github.lucastorri.moca.role.master

import akka.actor._
import akka.pattern.ask
import akka.persistence._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.snapshot.SnapshotStore
import akka.util.Timeout
import com.github.lucastorri.moca.role.Messages._
import com.github.lucastorri.moca.role.{Task, Work}
import com.github.lucastorri.moca.store.content.ContentLinksTransfer
import com.github.lucastorri.moca.store.control.{FakeTransfer, FakeCriteria, RunControl}
import com.github.lucastorri.moca.url.Url
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, MustMatchers}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Success, Try}

class MasterTest extends FlatSpec with MustMatchers {

  val port = 9898
  val name = "MasterTest"

  implicit val timeout: Timeout = 5.seconds
  implicit val system = ActorSystem(name, ConfigFactory.parseString(
    s"""
      |akka {
      |
      |  actor {
      |    provider = "akka.cluster.ClusterActorRefProvider"
      |  }
      |
      |  cluster {
      |    roles = ["master"]
      |    seed-nodes = ["akka.tcp://$name@127.0.0.1:$port"]
      |    auto-down-unreachable-after = 10s
      |  }
      |
      |  remote {
      |    netty.tcp {
      |      port = $port
      |      hostname = "127.0.0.1"
      |    }
      |  }
      |
      |}
      |
      |moca.master.journal-plugin-id = "journal"
      |moca.master.snapshot-plugin-id = "snapshot"
      |
      |journal {
      |  class = "com.github.lucastorri.moca.role.master.EmptyJournal"
      |  plugin-dispatcher = "akka.actor.default-dispatcher"
      |}
      |
      |snapshot {
      |  class = "com.github.lucastorri.moca.role.master.EmptySnapshotStore"
      |  plugin-dispatcher = "akka.actor.default-dispatcher"
      |}
    """.stripMargin))

  it must "close control before dying" in new context {
    withMaster { master =>

      master ! PoisonPill
      result(control.closed) must be (true)

    }
  }

  it must "add new work" in new context {
    withMaster { master =>

      val newWork = Set(Work("1", Url("http://www.example.com"), FakeCriteria))

      master ! AddWork(newWork)
      master ! PoisonPill

      result(control.closed)

      control.workAdded.head must equal (newWork)

    }
  }

  it must "ack when a task is finished" in new context {
    withMaster { master =>

      result(master ? TaskFinished(null, "1", FakeTransfer())) must be (Ack)

    }
  }

  it must "not offer two tasks with the same partition" in new context {
    withMaster { master =>

      val task1 = Task("1", Set(Url("http://www.example.com")), FakeCriteria, 0, "a")
      val task2 = Task("2", Set(Url("http://www.example.com")), FakeCriteria, 0, "a")
      control.publish(Set(task1))
      control.publish(Set(task2))

      val (m1, reply1) = requestTask(master)
      reply1 must equal (task1)

      val (m2, reply2) = requestTask(master)
      reply2 must be (Nack)

      master ! TaskFinished(m1.ref, "1", FakeTransfer())

      val (m3, reply3) = requestTask(master)
      reply3 must equal (task2)

      control.taskDone must equal (Seq("1"))

      Seq(m1, m2, m3).foreach(_.close())

    }
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

  def result[R](f: Future[R]): R =
    Await.result(f, timeout.duration)

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

class EmptySnapshotStore extends SnapshotStore {

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    Future.successful(None)

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    Future.successful(())

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    Future.successful(())

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    Future.successful(())

}

class EmptyJournal extends AsyncWriteJournal {

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] =
    Future.successful(messages.map(_ => Success(())))

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(())

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future.successful(0L)

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(recoveryCallback: (PersistentRepr) => Unit): Future[Unit] =
    Future.successful(())

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

  override def subTasks(parentTaskId: String, depth: Int, urls: Set[Url]): Future[Unit] = ???

  override def links(workId: String): Future[Option[ContentLinksTransfer]] = ???

  override def done(taskId: String, transfer: ContentLinksTransfer): Future[Option[String]] = {
    taskDone.append(taskId)
    Future.successful(None)
  }


  override def abort(taskIds: Set[String]): Future[Unit] = ???

  override def close(): Unit =
    _closed.success(true)

}