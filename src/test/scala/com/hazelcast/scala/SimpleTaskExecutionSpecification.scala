package com.hazelcast.scala

import scala.collection.JavaConverters._
import HazelcastDistributedTask._
import com.hazelcast.core.{MembershipEvent, MembershipListener, HazelcastInstance, Hazelcast}
import org.specs2.specification.{AfterExample, BeforeAfterExample}
import org.specs2.mutable.{After, BeforeAfter, Specification}
import java.util.concurrent.{TimeUnit, CountDownLatch, ExecutorService}

/**
 * User: remeniuk
 */

trait instance extends HazelcastInstances {
  val additionalInstancesCount = 1
}

class SimpleTaskExecutionSpecification extends Specification {

  /*"Synchronously execute simple task on local member" in {
    (distributedTask(Hazelcast.getCluster.getLocalMember) {
      1 + 1
    }).execute().get must beEqualTo(2)
  }

  "Asynchronously transforms, and synchronously waits" in {
    (distributedTask(Hazelcast.getCluster.getLocalMember) {
      1 + 1
    }).map(Hazelcast.getCluster.getLocalMember) {
      value: Int =>
        "Result: " + value.toString
    }.map(Hazelcast.getCluster.getLocalMember) {
      value: String => value.toString.reverse
    }
      .execute().get must beEqualTo("Result: 2".reverse)
  }

  "Synchronously execute simple task on 2 members synchronously" in new instance {
    Hazelcast.getCluster.getMembers.asScala.toList.map {
      member =>
        (distributedTask(member) {
          1
        }).execute().get
    }.sum must beEqualTo(2)
  } */

  "Tasks can be flattened" in {
    ((distributedTask(Hazelcast.getCluster.getLocalMember) {
      1
    }).flatMap(Hazelcast.getCluster.getLocalMember) {
      x: Int =>
        println("====== A ======")
        distributedTask(Hazelcast.getCluster.getLocalMember) {
          println("====== B ======")
          x + 1
        }
    }).execute().get must be equalTo 2
  }

  step {
    Hazelcast.shutdownAll()
  }

}