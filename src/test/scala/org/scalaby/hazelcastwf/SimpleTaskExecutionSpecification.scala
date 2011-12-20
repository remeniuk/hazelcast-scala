package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._

/**
 * User: remeniuk
 */


class SimpleTaskExecutionSpecification extends Specification {

  "Execute simple task on local member" in {
    distributedTask {
      () => 1 + 1
    }.onMember(Hazelcast.getCluster.getLocalMember)().get must be equalTo 2
  }

  "Apply transformation to a simple task" in {
    distributedTask {
      () =>
        1
    }.onMember(Hazelcast.getCluster.getLocalMember)
      .map {
      x =>
        x + 1
    }.onMember(Hazelcast.getCluster.getLocalMember)
      .map {
      x =>
        x * 2
    }.onMember(Hazelcast.getCluster.getLocalMember)().get must be equalTo 4
  }

  "Apply flattening transformation" in {
    distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)
      .flatMap {
      x => distributedTask {
        () => x + 1
      }
    }.onMember(Hazelcast.getCluster.getLocalMember)().get must be equalTo 2
  }

  "Reduce tasks in a parallel" in {
    val taskA = distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)

    val taskB = distributedTask {
      () => "2"
    }.onMember(Hazelcast.getCluster.getLocalMember)

    join(taskA, taskB)(_.toString + _).map(_.toInt).apply().get must be equalTo 12
  }

  step {
    Hazelcast.shutdownAll()
  }

}