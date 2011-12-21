package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._

/**
 * User: remeniuk
 */

object Pi {

  val N = 10000

  def calculate(start: Int): Double = (start until (start + N))
    .map(i => 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)).sum

}

class SimpleTaskExecutionSpecification extends Specification {

  "Execute simple task on local member" in {
    distributedTask {
      () => 1 + 1
    }.onMember(Hazelcast.getCluster.getLocalMember)() must be equalTo 2
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
    }.onMember(Hazelcast.getCluster.getLocalMember)() must be equalTo 4
  }

  "Apply flattening transformation" in {
    distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)
      .flatMap {
      x => distributedTask {
        () => x + 1
      }
    }.onMember(Hazelcast.getCluster.getLocalMember)() must be equalTo 2
  }

  "Join tasks in a parallel" in {
    val taskA = distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)

    val taskB = distributedTask {
      () => "2"
    }.onMember(Hazelcast.getCluster.getLocalMember)

    join(taskA, taskB)(_.toString + _).map(_.toInt)() must be equalTo 12
  }

  "Reduce tasks in a parallel (Pi calculation)" in {

    val result = reduce(
      Seq.fill(5)(Hazelcast.getCluster.getLocalMember).zipWithIndex.map {
        case (member, index) =>
          distributedTask {
            () => Pi.calculate(index * Pi.N)
          } onMember member
      }
    )(_ + _)()

    result must be closeTo (3.141 +/- 0.001)

  }

  step {
    Hazelcast.shutdownAll()
  }

}