package org.scalaby.hazelcastwf

import com.hazelcast.core.{Member, Hazelcast}
import scala.collection.JavaConverters._


/**
 * User: remeniuk
 */

object HazelcastUtil {

  def locked[T](lockId: AnyRef)(f: => T): T = {
    val lock = Hazelcast.getLock(lockId)
    lock.lock()
    try {
      f
    } finally {
      lock.unlock()
    }
  }

  def clusterMembers: Set[Member] = Hazelcast.getCluster.getMembers.asScala.toSet

  def clusterMembersList: List[Member] = clusterMembers.toList

}