package com.hazelcast.scala

import com.hazelcast.impl.base.DistributedLock
import scalaz._
import Scalaz._
import com.hazelcast.core.{ExecutionCallback, DistributedTask, Hazelcast, Member}
import java.util.concurrent._

/**
 * User: remeniuk
 */

object HazelcastDistributedTask {

  implicit def toCallable[T](f: () => T): Callable[T] with Serializable =
    new Callable[T] with Serializable {
      def call() = f()
    }

  implicit def toCallableTransformation[T, K](f: T => K): CallableTransformation[T, K] =
    new CallableTransformation[T, K] {
      def transform(from: T) = f(from)
    }

  def distributedTask[T](member: Member)
                        (callable: Callable[T] with Serializable): HazelcastDistributedTask[T, ExecutionNotStarted] =
    DistributedTaskWrapper[T, ExecutionNotStarted](new DistributedTask[T](callable, member), None)

  def distributedTask[T](key: AnyVal)
                        (callable: Callable[T] with Serializable): HazelcastDistributedTask[T, ExecutionNotStarted] =
    DistributedTaskWrapper[T, ExecutionNotStarted](new DistributedTask[T](callable, key), None)

}

trait CallableTransformation[T, K] extends Callable[K] with Serializable {

  var value: T = _

  def transform(from: T): K

  def call(): K = transform(value)

}

trait HazelcastDistributedTask[T, TaskState <: TaskExecutionState] {

  val executionStarted = new CountDownLatch(1)

  val distributedTaskInstance: DistributedTask[T]

  val rootTask: Option[HazelcastDistributedTask[_, _]]

  def execute(): HazelcastDistributedTask[T, ExecutionStarted] = {
    rootTask.map(_.executeSelf()).getOrElse(executeSelf())
    new DistributedTaskWrapper[T, ExecutionStarted](distributedTaskInstance, rootTask) {
      override val executionStarted = HazelcastDistributedTask.this.executionStarted
    }
  }

  def executeSelf() = if (executionStarted.getCount == 1) {
    Hazelcast.getExecutorService.execute(distributedTaskInstance)
    executionStarted.countDown()
  }

  def map[K](member: Member)(transform: CallableTransformation[T, K]): HazelcastDistributedTask[K, ExecutionNotStarted] = {
    val transformedTask = DistributedTaskWrapper[K, ExecutionNotStarted](new DistributedTask[K](transform, member),
      rootTask.orElse(Option(this)))

    distributedTaskInstance.setExecutionCallback(new ExecutionCallback[T] {
      def done(future: Future[T]) = {
        transform.value = future.get
        transformedTask.executeSelf()
      }
    })

    transformedTask
  }

  def get(implicit stateCheck: TaskState =:= ExecutionStarted): T = {
    executionStarted.await()
    distributedTaskInstance.get
  }

}

case class DistributedTaskWrapper[T, TaskState <: TaskExecutionState](distributedTaskInstance: DistributedTask[T],
                                                                      rootTask: Option[HazelcastDistributedTask[_, _]]) extends HazelcastDistributedTask[T, TaskState]

