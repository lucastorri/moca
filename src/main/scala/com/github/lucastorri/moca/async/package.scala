package com.github.lucastorri.moca

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

package object async {

  def retry[T](times: Int)(action: => Future[T])(implicit ctx: ExecutionContext): Future[T] = {

    val p = Promise[T]()

    def attempt = Try(action) match {
      case Success(v) => v
      case Failure(t) => Future.failed(t)
    }

    def retries(left: Int): Unit = {
      attempt.onComplete {
        case Success(v) => p.success(v)
        case Failure(t) if left > 0 => retries(left - 1)
        case Failure(t) => p.failure(t)
      }
    }

    retries(times)
    p.future
  }

  def runnable(f: => Unit): Runnable =
    new Runnable {
      override def run(): Unit = f
    }

  def spawn[T](f: => T): Future[T] = {
    val promise = Promise[T]()
    new Thread() {
      override def run(): Unit = promise.complete(Try(f))
    }.start()
    promise.future
  }

  def noop[E](e: E): Unit = {}

}
