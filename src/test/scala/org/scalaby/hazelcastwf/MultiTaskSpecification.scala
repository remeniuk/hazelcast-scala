package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._
import scala.collection.JavaConverters._


class MultiTaskSpecification extends Specification {

  step {
    TestSuite.startTest
  }

  "Reduce multitask" in additionalInstance {
    val membersCount = HazelcastUtil.clusterMembers.size
    multiTask(HazelcastUtil.clusterMembers)(() => 1)
      .map(_.sum)().get must be equalTo membersCount
  }

  "Mapping to a fork/complete multitask" in additionalInstance {
    val membersCount = HazelcastUtil.clusterMembers.size

    val root = multiTask(HazelcastUtil.clusterMembers)(() => 1)

    val task1 = root.map(_.sum)
    val task2 = root.map(_.foldLeft(1)(_ * _))

    task1().get must be equalTo membersCount
    task2().get must be equalTo 1
  }

  step {
    TestSuite.stopTest
  }

}