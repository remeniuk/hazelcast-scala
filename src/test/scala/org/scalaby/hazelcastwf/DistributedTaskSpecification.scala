package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger

/**
 * User: remeniuk
 */

object Pi {

  val N = 10000

  def calculate(start: Int): Double = (start until (start + N))
    .map(i => 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)).sum

}

class DistributedTaskSpecification extends Specification {

  step {
    TestSuite.startTest
  }

  /*"Execute simple task on local member" in {
    distributedTask {
      () => 1 + 1
    }.onMember(Hazelcast.getCluster.getLocalMember)().get must be equalTo 2
  } */

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

  "Join tasks in a parallel" in {
    val taskA = distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)

    val taskB = distributedTask {
      () => "2"
    }.onMember(Hazelcast.getCluster.getLocalMember)

    val taskC = distributedTask {
      () => 1
    }.onMember(Hazelcast.getCluster.getLocalMember)

    taskA
      .join(taskB)(_.toString + _)
      .join(taskC)(_.length + _)
      .map(_.toDouble)().get must be equalTo 3d
  }

  "Reduce tasks in a parallel (Pi calculation)" in additionalInstance {

    val result = reduce(
      HazelcastUtil.clusterMembersList.zipWithIndex.map {
        case (member, index) =>
          distributedTask {
            () => Pi.calculate(index * Pi.N)
          } onMember member
      }
    )(_ + _)()

    result.get must be closeTo (3.141 +/- 0.001)

  }

  "Mapping to a fork/complete task" in {

    val callsCount = Hazelcast.getAtomicNumber("mapping-to-a-complete-task")

    val root = distributedTask {
      () =>
        callsCount.incrementAndGet()
        1
    }

    val task1 = root.map {
      x =>
        callsCount.incrementAndGet()
        "r:" + x.toString
    }

    val task2 = root.map {
      x =>
        x * 2
    }

    task1().get must be equalTo "r:1"
    task2().get must be equalTo 2
    callsCount.get() must be equalTo 2

    task2.map {
      x =>
        x * 2
    }().get must be equalTo 4

  }

  step {
    TestSuite.stopTest
  }

}