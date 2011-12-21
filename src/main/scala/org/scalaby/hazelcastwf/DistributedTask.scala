package org.scalaby.hazelcastwf

import com.hazelcast.core.{ExecutionCallback, Member, Hazelcast, DistributedTask => HazelcastDistributedTask}
import java.util.concurrent.Future
import com.google.common.base.Objects

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

  def reduce[T](tasks: Iterable[DistributedTask[T]])(f: (T, T) => T): DistributedTask[T] = {
    val res = new ExecuteTaskRandomly[T]() {
      override implicit val context =
        ((tasks.head.context /: tasks.tail) {
          (c, x) => c.join(x.context).addDependency(x, this)
        }).addTask(this)
    }
    Promises.put(res.id, new FoldablePromise[T](res.id, f, tasks.size))

    res
  }

  def join[A: Manifest, B, C](a: DistributedTask[A], b: DistributedTask[B])(f: (A, B) => C): DistributedTask[C] = {
    val res = new ExecuteTaskRandomly[C]() {
      override implicit val context =
        (a.context join b.context)
          .addDependency(a, this)
          .addDependency(b, this)
          .addTask(this)
    }
    Promises.put(res.id, new Promise2[A, B, C](res.id, f))

    res
  }

}

trait DistributedTask[T] {

  import DistributedTask._

  val id = generateTaskId

  val parent: Option[DistributedTask[_]]

  implicit val thisTask: Option[DistributedTask[_]] = Some(this)

  implicit val context: Context = (parent.map {
    p => p.context.addDependency(p, this)
  } getOrElse (Context()))
    .addTask(this)

  def createDistributedTask = {
    val promise = Promises.get[T](id)
    if (promise.isCallable) Some(new HazelcastDistributedTask[T](promise))
    else None
  }

  def execute(ctx: Context): Unit =
    createDistributedTask.map {
      task =>

        ctx.getDependecies(this).foreach {
          dependency =>
            task.setExecutionCallback(new ExecutionCallback[T] {
              def done(future: Future[T]) = {
                val promiseLock = Hazelcast.getLock("promise:" + dependency.id)
                promiseLock.lock()
                try {
                  Promises.get[T](dependency.id) match {
                    case promise: Promise2[T, _, _] =>
                      Promises.put(dependency.id, promise.curry(future.get))
                    case promise: Promise1[T, _] =>
                      Promises.put(dependency.id, promise.curry(future.get))
                    case promise: FoldablePromise[T] =>
                      Promises.put(dependency.id, promise.fold(future.get))
                    case _ => throw new IllegalStateException("Unsupported promise type!")
                  }
                } finally {
                  promiseLock.unlock()
                }
                dependency.execute(ctx)
              }
            })
        }

        executor.execute(task)
    }

  def apply() = {
    context.roots.foreach(_.execute(context))
    this
  }

  def get = Promises.get[T](id).get

  def onMember(member: Member): DistributedTask[T] = ExecuteDistributedTaskOnMember[T](member, id)(parent)

  def map[V](f: T => V): DistributedTask[V] = {
    val task = ExecuteTaskRandomly[V]()
    Promises.put(task.id, new Promise1[T, V](task.id, f))
    task
  }

  def flatMap[V](f: T => DistributedTask[V]): DistributedTask[V] = {
    val unflattenedTask = ExecuteTaskRandomly[DistributedTask[V]]()
    val flattenedTask = ExecuteTaskRandomly[V]()(Some(unflattenedTask))

    Promises.put(flattenedTask.id, new Promise1[DistributedTask[V], V](flattenedTask.id, {
      task: DistributedTask[V] => task().get
    }
    ))

    Promises.put(unflattenedTask.id, new Promise1[T, DistributedTask[V]](unflattenedTask.id, f))

    flattenedTask
  }

  override def hashCode() = Objects.hashCode(id)

  override def equals(p1: Any) =
    if (p1.isInstanceOf[DistributedTask[_]])
      Objects.equal(id, p1.asInstanceOf[DistributedTask[_]].id)
    else false

  override def toString = id

}

case class ExecuteTaskRandomly[T](implicit parent: Option[DistributedTask[_]] = None) extends DistributedTask[T]

case class ExecuteDistributedTaskOnMember[T](member: Member,
                                             override val id: String = DistributedTask.generateTaskId)
                                            (implicit val parent: Option[DistributedTask[_]] = None)
  extends DistributedTask[T] {

  override def createDistributedTask = {
    val promise = Promises.get[T](id)
    if (promise.isInstanceOf[Promise0[_]]) Some(new HazelcastDistributedTask[T](promise, member))
    else None
  }

}


