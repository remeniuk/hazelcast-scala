package com.hazelcast.scala

import scala.collection.JavaConverters._
import com.hazelcast.core.{MembershipEvent, MembershipListener, HazelcastInstance, Hazelcast}
import org.specs2.specification.{AfterExample, BeforeAfterExample}
import org.specs2.mutable.{After, BeforeAfter, Specification}
import java.util.concurrent.{TimeUnit, CountDownLatch, ExecutorService}
import com.hazelcast.scala.HazelcastDistributedTask._

/**
 * User: remeniuk
 */

object Pi {

  val N = 10000

  def calculate(start: Int): Double = (start until (start + N))
    .map(i => 4.0 * (1 - (i % 2) * 2) / (2 * i + 1)).sum

}

class PiCalculationExample extends Specification {

  trait manyInstances extends HazelcastInstances {
    val additionalInstancesCount = 1
  }

  "Calculates Pi number in the distributed context" in new manyInstances {
    val clusterMembers = Hazelcast.getCluster.getMembers.asScala.toList

    val result = clusterMembers.zipWithIndex.map {
      case (member, index) =>
        distributedTask(member) {
          () => Pi.calculate(index * Pi.N)
        }.execute().get
    } sum

    println("Result: " + result)

    result must be closeTo(3.141 +/- 0.001)
  }

  step {
    Hazelcast.shutdownAll()
  }

}