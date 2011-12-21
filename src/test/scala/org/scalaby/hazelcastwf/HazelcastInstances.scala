package org.scalaby.hazelcastwf

import org.specs2.mutable.BeforeAfter
import java.util.concurrent.CountDownLatch
import com.hazelcast.core.{HazelcastInstance, Hazelcast, MembershipEvent, MembershipListener}

/**
 * User: remeniuk
 */

trait additionalInstance extends HazelcastInstances {

  val additionalInstancesCount = 1

}

trait HazelcastInstances extends BeforeAfter {

  val additionalInstancesCount: Int

  val startupLatch = new CountDownLatch(additionalInstancesCount)

  private var additionalInstances: Iterable[HazelcastInstance] = _

  val membershipListener = new MembershipListener {
    def memberAdded(membershipEvent: MembershipEvent) {
      startupLatch.countDown()
    }

    def memberRemoved(membershipEvent: MembershipEvent) {}
  }

  def before {
    Hazelcast.getCluster.addMembershipListener(membershipListener)

    additionalInstances = (1 to additionalInstancesCount).map(_ => Hazelcast.newHazelcastInstance(null))

    startupLatch.await()
  }

  def after {
    Hazelcast.getCluster.removeMembershipListener(membershipListener)
    additionalInstances.foreach(_.getLifecycleService.shutdown())
  }
}