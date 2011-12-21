package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast

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

}