package org.scalaby.hazelcastwf

import java.util.concurrent.TimeUnit

/**
 * User: remeniuk
 */

case class Context(dependencies: Map[String, Seq[String]] = Map(),
                   tasks: Map[String, DistributedTask[_]] = Map()) {

import java.util.concurrent.TimeUnit

def addTask(task: DistributedTask[_]) =
    copy(tasks = tasks + (task.id -> task))

  def addDependency(from: DistributedTask[_], to: DistributedTask[_]) = {
    val taskDependencies = dependencies.get(from.id).getOrElse(Seq()) :+ to.id
    copy(dependencies = dependencies + (from.id -> taskDependencies))
  }

  def getDependecies(of: DistributedTask[_]): Seq[DistributedTask[_]] =
    dependencies.get(of.id).map {
      deps =>
        deps.flatMap(dep => tasks.get(dep))
    } getOrElse (Seq())

  def roots = {
    val depTasks = dependencies.values.flatten.toSeq
    tasks.filterNot(e => depTasks.contains(e._1)).values
  }

  def join(that: Context) = copy(
    dependencies = (this.dependencies.keys ++ that.dependencies.keys)
      .map {
      key => key -> (this.dependencies.get(key).getOrElse(Seq()) ++ that.dependencies.get(key).getOrElse(Seq()))
    }.toMap,
    tasks = this.tasks ++ that.tasks
  )

}