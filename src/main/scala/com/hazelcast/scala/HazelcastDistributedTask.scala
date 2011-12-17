package com.hazelcast.scala

import com.hazelcast.impl.base.DistributedLock
import scalaz._
import Scalaz._
import com.hazelcast.core.{ExecutionCallback, DistributedTask, Hazelcast, Member}
import java.util.concurrent._

/*case class TransformOnMember[A, B](f: A => B, member: Member) extends Function[A, B] {
  override def apply(value: A) = f(value)
} */

/**
 * User: remeniuk
 */

object HazelcastDistributedTask extends Applys {

  type NotStartedHazelcastTask[T] = HazelcastDistributedTask[T, ExecutionNotStarted]
  type StartedHazelcastTask[T] = HazelcastDistributedTask[T, ExecutionStarted]

  implicit def HazelcastDistributedTaskFunctor: Functor[NotStartedHazelcastTask] = new Functor[NotStartedHazelcastTask] {
    def fmap[A, B](r: NotStartedHazelcastTask[A], f: Function[A, B]) = r.map(null)(f)
  }

  /*implicit def HazelcastDistributedTaskBind: Bind[NotStartedHazelcastTask] = new Bind[NotStartedHazelcastTask] {
    def bind[A, B](a: NotStartedHazelcastTask[A], f: (A) => NotStartedHazelcastTask[B]): NotStartedHazelcastTask[B] = {
      f(a.value)
    }
  } */

  implicit def toCallable[T](f: => T): Callable[T] with Serializable =
    new Callable[T] with Serializable {
      def call() = f
    }

  implicit def toCallableTransformation[T, K](f: T => K): CallableTransform[T, K] =
    new CallableTransform[T, K] {
      def transform(from: T) = f(from)
    }

  def distributedTask[T](member: Member)
                        (callable: Callable[T] with Serializable): HazelcastDistributedTask[T, ExecutionNotStarted] =
    DistributedTaskWrapper[T, ExecutionNotStarted](() => new DistributedTask[T](callable, member), None)

  /*def distributedTask[T](key: AnyVal)
                        (callable: Callable[T] with Serializable): HazelcastDistributedTask[T, ExecutionNotStarted] =
    DistributedTaskWrapper[T, ExecutionNotStarted](new DistributedTask[T](callable, key), None)*/

  /*def flatten[T](task: NotStartedHazelcastTask[NotStartedHazelcastTask[T]]): NotStartedHazelcastTask[T] = {
    task.distributedTaskInstance.setExecutionCallback(new ExecutionCallback[HazelcastDistributedTask.NotStartedHazelcastTask[T]] {
      def done(future: Future[HazelcastDistributedTask.NotStartedHazelcastTask[T]]) {}
    })
    */
  //val transformedTask = DistributedTaskWrapper[T, ExecutionNotStarted](new DistributedTask[T](transform, member),
  //  rootTask.orElse(Option(this)))

  //task.value.
  //transformedTask
  //}

}

trait CallableTransform[T, K] extends Callable[K] with Serializable {

  var value: T = _

  def transform(from: T): K

  def call(): K = transform(value)

}

trait HazelcastDistributedTask[T, TaskState <: TaskExecutionState] {

  import HazelcastDistributedTask._

  @transient var _executionStarted = new CountDownLatch(1)

  def executionStarted = {
    if (_executionStarted == null) _executionStarted = new CountDownLatch(1)
    _executionStarted
  }

  @transient
  @volatile protected var _distributedTaskInstance: DistributedTask[T] = _

  def distributedTaskInstance: DistributedTask[T] = {
    if (_distributedTaskInstance == null) _distributedTaskInstance = distributedTaskInstanceConstructor()
    _distributedTaskInstance
  }

  val distributedTaskInstanceConstructor: () => DistributedTask[T]

  val rootTask: Option[HazelcastDistributedTask[_, _]]

  def execute(): StartedHazelcastTask[T] = {
    rootTask.map(_.executeSelf()).getOrElse(executeSelf())
    new DistributedTaskWrapper[T, ExecutionStarted](() => distributedTaskInstance, rootTask) {
      _executionStarted = HazelcastDistributedTask.this._executionStarted
    }
  }

  def executeSelf() = if (executionStarted.getCount == 1) {
    Hazelcast.getExecutorService.execute(distributedTaskInstance)
    executionStarted.countDown()
  }

  def map[K](member: Member)(transform: CallableTransform[T, K]): NotStartedHazelcastTask[K] = {
    val transformedTask = DistributedTaskWrapper[K, ExecutionNotStarted](() => new DistributedTask[K](transform, member),
      rootTask.orElse(Option(this)))

    distributedTaskInstance.setExecutionCallback(new ExecutionCallback[T] {
      def done(future: Future[T]) = {
        transform.value = future.get(5, TimeUnit.SECONDS)
        transformedTask.executeSelf()
      }
    })

    transformedTask
  }

  def flatMap[K](member: Member)(transform: CallableTransform[T, NotStartedHazelcastTask[K]]): NotStartedHazelcastTask[K] = {
    val unflattenedTask = DistributedTaskWrapper[NotStartedHazelcastTask[K], ExecutionNotStarted](() => new DistributedTask[NotStartedHazelcastTask[K]](transform, member),
      rootTask.orElse(Option(this)))

    val indentity: CallableTransform[K, K] = (x: K) => x

    val flattenedTask = DistributedTaskWrapper[K, ExecutionNotStarted](() => new DistributedTask[K](indentity, member),
      rootTask.orElse(Option(this)))

    distributedTaskInstance.setExecutionCallback(new ExecutionCallback[T] {
      def done(future: Future[T]) = {
        println("====== 1 ======")
        transform.value = future.get(5, TimeUnit.SECONDS)
        unflattenedTask.executeSelf()
      }
    })

    unflattenedTask.distributedTaskInstance.setExecutionCallback(new ExecutionCallback[HazelcastDistributedTask.NotStartedHazelcastTask[K]] {
      def done(future: Future[HazelcastDistributedTask.NotStartedHazelcastTask[K]]) {
        println("====== 2 ======")
        future.get.distributedTaskInstance.setExecutionCallback(new ExecutionCallback[K] {
          def done(future: Future[K]) {
            println("====== 3 ======")
            indentity.value = future.get(5, TimeUnit.SECONDS)
            flattenedTask.executeSelf()
          }
        })
        future.get.executeSelf()
      }
    })

    flattenedTask
  }

  def get(implicit stateCheck: TaskState =:= ExecutionStarted): T = {
    assert(executionStarted.await(5, TimeUnit.SECONDS))
    distributedTaskInstance.get(5, TimeUnit.SECONDS)
  }

}

case class DistributedTaskWrapper[T, TaskState <: TaskExecutionState](val distributedTaskInstanceConstructor: () => DistributedTask[T],
                                                                      rootTask: Option[HazelcastDistributedTask[_, _]]) extends HazelcastDistributedTask[T, TaskState]

