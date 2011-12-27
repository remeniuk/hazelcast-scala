package org.scalaby.hazelcastwf

import com.hazelcast.core.{ExecutionCallback, Member, DistributedTask => HazelcastDistributedTask}
import java.util.concurrent.{TimeUnit, CopyOnWriteArrayList, Callable, Future}
import scala.collection.JavaConverters._

/**
 * User: remeniuk
 */

class HazelcastMultiTask[T](id: String, callable: Callable[Any], members: Set[Member])
  extends HazelcastDistributedTask[Any](callable, members.asJava) with TaskCancellation {

  override lazy val topicId = id

  protected var results = new CopyOnWriteArrayList[T]

  override def onResult(result: Any): Unit = results.add(result.asInstanceOf[T])

  override def get: java.util.Collection[T] = {
    super.get
    results
  }

  override def get(timeout: Long, unit: TimeUnit): java.util.Collection[T] = {
    super.get(timeout, unit)
    results
  }

}

case class MultiTask[T](members: Set[Member],
                        override val id: String = DistributedTask.generateTaskId)
                       (implicit val parent: Option[DistributedTask[_]] = None)
  extends DistributedTask[Iterable[T]] {

  import DistributedTask._

  lazy val innerTask = new HazelcastMultiTask[T](id, Promises.get[Any](id), members)

  private def partiallyApplyDependency[K](dependency: DistributedTask[_], value: Any, ctx: Context) = {
    HazelcastUtil.locked("promise:" + dependency.id) {
      Promises.get[Iterable[T]](dependency.id) match {
        case promise: PartiallyAppliable[Iterable[T], _] =>
          Promises.put(dependency.id, promise.partiallyApply(innerTask.get.asScala))
        case _ => throw new IllegalStateException("Unsupported promise type!")
      }
    }
    dependency.execute(ctx)
  }

  private def executeCompleteTask(ctx: Context, value: Any) =
    ctx.getDependecies(this).foreach(partiallyApplyDependency(_, value, ctx))

  override def execute(ctx: Context) = {
    result.map(executeCompleteTask(ctx, _))
      .getOrElse {
      ctx.getDependecies(this).foreach {
        dependency =>
          innerTask.setExecutionCallback(new ExecutionCallback[Any] {
            def done(future: Future[Any]) = {
              partiallyApplyDependency(dependency, future.get(), ctx)
            }
          })
      }

      executor.execute(innerTask)
    }
  }

  private def innerGet(f: => Iterable[T], cancelOnTimeout: Boolean = false): Iterable[T] = result.getOrElse {
    apply()
    result = Some(f)
    Promises.cleanup(this)
    result.get
  }

  override def get: Iterable[T] = innerGet(innerTask.get.asScala)

  override def get(timeout: Long, unit: TimeUnit): Iterable[T] = innerGet(innerTask.get(timeout, unit).asScala)

}
