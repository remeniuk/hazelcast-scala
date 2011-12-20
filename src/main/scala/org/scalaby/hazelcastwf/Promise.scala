package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import java.util.concurrent.{TimeUnit, Callable}

/**
 * User: remeniuk
 */

trait Promise[T] extends Callable[T] with Serializable {

  import DistributedTask._

  val taskId: String

  def countDownLatch = {
    val c = Hazelcast.getCountDownLatch(taskId)
    c.setCount(1)
    c
  }

  def get = {
    countDownLatch.await(5, TimeUnit.SECONDS)
    getPromisedValue[T](taskId)
  }

  def isFulfilled = !countDownLatch.hasCount

}

class Promise0[T](val taskId: String, f0: () => T) extends Promise[T] {

  import DistributedTask._

  def call() = {
    val result = f0()
    fulfill(result)
    result
  }

  def fulfill(result: T) = {
    storePromisedValue(taskId, result)
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
