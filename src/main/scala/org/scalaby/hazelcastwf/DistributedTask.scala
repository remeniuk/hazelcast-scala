package org.scalaby.hazelcastwf

import com.google.common.base.Objects
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, CountDownLatch, Future}
import com.hazelcast.core.{MessageListener, ExecutionCallback, Member, Hazelcast, DistributedTask => HazelcastDistributedTask}

/**
 * User: remeniuk
 */


object DistributedTask {

  final val TASK_PREFIX = "wf-task:"
  final val ID_GENERATOR = "hazelcast-wh"
  final val PROMISES_EXECUTOR = "wf-promises-executor"
  final val TASK_DEPENDENCIES = "wf-task-deps"

  def generateTaskId = TASK_PREFIX + Hazelcast.getIdGenerator(ID_GENERATOR).newId()

  def executor = Hazelcast.getExecutorService(PROMISES_EXECUTOR)

  def distributedTask[T](task: () => T): DistributedTask[T] = {
    val res = ExecuteTaskRandomly[T]()
    Promises.put(res.id, new Promise0[T](res.id, task))
    res
  }

  def multiTask[T](members: Set[Member])(task: () => T): DistributedTask[Iterable[T]] = {
    val res = MultiTask[T](members)
    Promises.put(res.id, new Promise0[T](res.id, task))
    res
  }

  def reduce[T](tasks: Iterable[DistributedTask[T]])(f: (T, T) => T): DistributedTask[T] = {
    val res = new ExecuteTaskRandomly[T]()

    Promises.put(res.id, new FoldablePromise[T](res.id, f, tasks.size))

    res.withContext(((tasks.head.context /: tasks.tail) {
      (c, x) => c.join(x.context).addDependency(x, res)
    }).addDependency(tasks.head, res).addTask(res))
  }

  def join[A, B: Manifest, C](a: DistributedTask[A], b: DistributedTask[B])(f: (A, B) => C): DistributedTask[C] = {
    val res = new ExecuteTaskRandomly[C]()

    Promises.put(res.id, new Promise2[A, B, C](res.id, f))

    res.withContext((a.context join b.context)
      .addDependency(a, res)
      .addDependency(b, res)
      .addTask(res))
  }

}

trait DistributedTask[T] {

  import DistributedTask._

  val id = generateTaskId

  val parent: Option[DistributedTask[_]]

  @volatile protected var result: Option[T] = None
  private val applied = new AtomicBoolean(false)

  private[hazelcastwf] def result(value: Any): Unit =
    result = Some(value.asInstanceOf[T])

  implicit val thisTask: Option[DistributedTask[_]] = Some(this)

  @volatile implicit var context: Context = (parent.map {
    p => p.context.addDependency(p, this)
  } getOrElse (Context()))
    .addTask(this)

  def createDistributedTask = {
    val promise = Promises.get[T](id)
    if (promise.isCallable) Some(
      new HazelcastDistributedTask[T](promise) with TaskCancellation {
        override lazy val topicId = id
      })
    else None
  }

  def isComplete = result.isDefined

  private def executeCompleteTask(ctx: Context, value: T) =
    ctx.getDependecies(this).foreach(partiallyApplyDependency(_, value, ctx))

  private def partiallyApplyDependency[K](dependency: DistributedTask[K], value: T, ctx: Context) = {
    if (!dependency.isComplete) {
      HazelcastUtil.locked("promise:" + dependency.id) {
        Promises.get[T](dependency.id) match {
          case promise: PartiallyAppliable[T, _] =>
            Promises.put(dependency.id, promise.partiallyApply(value))
          case _ => throw new IllegalStateException("Unsupported promise type!")
        }
      }
    }
    dependency.execute(ctx)
  }

  def execute(ctx: Context): Unit =
    result.map(executeCompleteTask(ctx, _))
      .getOrElse {
      createDistributedTask.map {
        task =>
          ctx.getDependecies(this).foreach {
            // FIXME: task can take only one callback. callbacks for different dependencies should be combined
            dependency =>
              task.setExecutionCallback(new ExecutionCallback[T] {
                def done(future: Future[T]) = partiallyApplyDependency(dependency, future.get, ctx)
              })
          }
          executor.execute(task)
      }
    }

  def apply(): DistributedTask[T] = {
    if (applied.compareAndSet(false, true))
      context.roots.foreach(_.execute(context))
    this
  }

  protected def get(f: Promise[T] => T, cancelOnTimeout: Boolean = false): T = result.getOrElse {
    apply()
    result = Some(f(Promises.get[T](id)))
    Promises.cleanup(this)
    result.get
  }

  def get: T = get(promise => promise.get)

  def get(timeout: Long, unit: TimeUnit): T = get(promise => promise.get(timeout, unit))

  def cancel() = TaskCancellation.cancel(id)

  def cancelAll() = context.tasks.keys.foreach(TaskCancellation.cancel)

  def onMember(member: Member): DistributedTask[T] = ExecuteDistributedTaskOnMember[T](member, id)(parent)

  def map[V](f: T => V): DistributedTask[V] = {
    val task = ExecuteTaskRandomly[V]()
    Promises.put(task.id, new Promise1[T, V](task.id, f))
    task
  }

  def flatMap[V](f: T => DistributedTask[V]): DistributedTask[V] = {
    val unflattenedTask = ExecuteTaskRandomly[DistributedTask[V]]()
    val flattenedTask = ExecuteTaskRandomly[V]()(Some(unflattenedTask))

    Promises.put(flattenedTask.id, new Promise1[DistributedTask[V], V](flattenedTask.id, _.get))

    Promises.put(unflattenedTask.id, new Promise1[T, DistributedTask[V]](unflattenedTask.id, f))

    flattenedTask
  }

  def join[B: Manifest, C](b: DistributedTask[B])(f: (T, B) => C): DistributedTask[C] =
    DistributedTask.join(this, b)(f)

  override def hashCode() = Objects.hashCode(id)

  override def equals(p1: Any) =
    if (p1.isInstanceOf[DistributedTask[_]])
      Objects.equal(id, p1.asInstanceOf[DistributedTask[_]].id)
    else false

  override def toString = id

  def withContext(ctx: Context) = {
    context = ctx
    this
  }

}

case class ExecuteTaskRandomly[T](implicit parent: Option[DistributedTask[_]] = None) extends DistributedTask[T]

case class ExecuteDistributedTaskOnMember[T](member: Member,
                                             override val id: String = DistributedTask.generateTaskId)
                                            (implicit val parent: Option[DistributedTask[_]] = None)
  extends DistributedTask[T] {

  override def createDistributedTask = {
    val promise = Promises.get[T](id)
    if (promise.isInstanceOf[Promise0[_]]) Some(new HazelcastDistributedTask[T](promise, member) with TaskCancellation {
      override lazy val topicId = id
    })
    else None
  }

}

