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
      .map(_.sum)() must be equalTo membersCount
  }

  step {
    TestSuite.stopTest
  }

}