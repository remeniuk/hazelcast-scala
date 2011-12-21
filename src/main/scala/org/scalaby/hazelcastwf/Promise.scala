package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import java.util.concurrent.{TimeUnit, Callable}
import com.hazelcast.impl.CountDownLatchProxy

/**
 * User: remeniuk
 */

trait Promise[T] extends Callable[T] with Serializable {

  import DistributedTask._

  val taskId: String

  def isCallable = false

  def countDownLatch = {
    val c = Hazelcast.getCountDownLatch(taskId)
    c.setCount(1)
    c
  }

  def get = {
    countDownLatch.await(5, TimeUnit.SECONDS)
    Promises.getResult[T](taskId)
  }

  def isFulfilled = !countDownLatch.hasCount

}

class Promise0[T](val taskId: String, f0: () => T) extends Promise[T] {

  import DistributedTask._

  override def isCallable = true

  def call() = {
    val result = f0()
    fulfill(result)
    result
  }

  def fulfill(result: T) = {
    Promises.putResult(taskId, result)
    countDownLatch.countDown()
  }

}

class Promise1[A, B](val taskId: String, f1: A => B)
  extends Promise[B] {

  def call() = throw new IllegalStateException("Cannot execute promise directly")

  def curry(value: A) = new Promise0[B](taskId, () => f1(value))

}

class Promise2[A, B, C](val taskId: String, f2: (A, B) => C)
                       (implicit first: ClassManifest[A])
  extends Promise[B] {

  def call() = throw new IllegalStateException("Cannot execute promise directly")

  def curry(value: Any) =
    if (value.asInstanceOf[AnyVal].getClass.isAssignableFrom(Primitives.box(first.erasure))) {
      new Promise1[B, C](taskId, f2(value.asInstanceOf[A], _))
    } else {
      new Promise1[A, C](taskId, f2(_, value.asInstanceOf[B]))
    }

}

case class FoldablePromise[T](taskId: String,
                              f: (T, T) => T,
                              count: Int,
                              state: T => T = (x: T) => x) extends Promise[T] {

  override def countDownLatch = {
    val c = Hazelcast.getCountDownLatch(taskId)
    if (c.asInstanceOf[CountDownLatchProxy].getCount == 0)
      c.setCount(count + 1)
    c
  }

  override def isCallable = countDownLatch.asInstanceOf[CountDownLatchProxy].getCount == 1

  def fold(value: T) = {
    if (Promises.getResult(taskId) == null) {
      Promises.putResult(taskId, value)
      this
    } else copy(state = state compose f.curried(value))
  }

  def call = {
    val result = state(Promises.getResult[T](taskId))
    Promises.putResult(taskId, result)
    countDownLatch.countDown()
    result
  }

}
