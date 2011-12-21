package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._
import scalaz._
import Scalaz._
import HazelcastScalaz._

/**
 * User: remeniuk
 */


class ScalazHazelacast extends Specification {

  /*"Reduce tasks sequentially" in {
    val taskA = distributedTask {
      () => "A"
    }.onMember(Hazelcast.getCluster.getLocalMember)

    val taskB = distributedTask {
      () => "B"
    }.onMember(Hazelcast.getCluster.getLocalMember)

    (taskA |@| taskB)(_ + _).apply() must be equalTo "AB"
  } */

  step {
    Hazelcast.shutdownAll()
  }

}