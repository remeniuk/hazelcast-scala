package com.hazelcast.scala

import org.specs2.mutable.BeforeAfter
import java.util.concurrent.CountDownLatch
import com.hazelcast.core.{Hazelcast, MembershipEvent, MembershipListener}

/**
 * User: remeniuk
 */

trait HazelcastInstances extends BeforeAfter {

  val additionalInstancesCount: Int

  val startupLatch = new CountDownLatch(additionalInstancesCount)

  val membershipListener = new MembershipListener {
    def memberAdded(membershipEvent: MembershipEvent) {
      startupLatch.countDown()
    }

    def memberRemoved(membershipEvent: MembershipEvent) {}
  }

  def before {
    Hazelcast.getCluster.addMembershipListener(membershipListener)

    (1 to additionalInstancesCount).foreach(_ => Hazelcast.newHazelcastInstance(null))

    startupLatch.await()
  }

  def after {
    Hazelcast.getCluster.removeMembershipListener(membershipListener)
  }
}