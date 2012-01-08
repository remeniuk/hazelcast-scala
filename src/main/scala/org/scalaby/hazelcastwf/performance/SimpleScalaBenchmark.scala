package org.scalaby.hazelcastwf.performance

/**
 * Copied from scala-benchmarking-template
 * https://github.com/sirthias/scala-benchmarking-template/blob/master/src/main/scala/org/example/SimpleScalaBenchmark.scala
 */

import com.google.caliper.SimpleBenchmark

trait SimpleScalaBenchmark extends SimpleBenchmark {

  def repeat[@specialized A](reps: Int)(snippet: => A) = {
    val zero = 0.asInstanceOf[A]
    var i = 0
    var result = zero
    while (i < reps) {
      val res = snippet
      if (res != zero) result = res
      i = i + 1
    }
    result
  }

}