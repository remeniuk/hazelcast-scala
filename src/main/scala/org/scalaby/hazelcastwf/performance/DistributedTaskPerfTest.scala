package org.scalaby.hazelcastwf.performance

import org.scalaby.hazelcastwf.DistributedTask._
import org.scalaby.hazelcastwf.HazelcastUtil._
//import com.google.caliper.api.{VmParam, Benchmark}
import com.google.caliper.{Param, SimpleBenchmark}

/**
 * User: remeniuk
 */

class DistributedTaskPerfTest extends SimpleScalaBenchmark {

  @Param(Array("1", "2", "3"))
  private var interval: Int = _

  def timeLocalNodeDistributedTask(reps: Int) =  repeat(reps){
    Thread.sleep(interval)
    1 + 1
    /*(distributedTask{
      () => 1 + 1
    } onMember(local))().get*/
  }

}