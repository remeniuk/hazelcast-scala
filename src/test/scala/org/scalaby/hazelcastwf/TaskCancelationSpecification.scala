package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast
import org.specs2.mutable.Specification
import DistributedTask._
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit

object TaskCancellationSpecification {

  def taskExecutionLatch(testIndex: Int) =
    Hazelcast.getCountDownLatch("task-cancellation-specification-%s" format (testIndex))

}

class TaskCancellationSpecification extends Specification {

  import TaskCancellationSpecification._

  step {
    TestSuite.startTest
  }

  "Long-running task" should {

    "complete running after a timeout" in {

      taskExecutionLatch(0).setCount(2)

      val task = distributedTask {
        () =>
          Thread.sleep(1000)
          taskExecutionLatch(0).countDown()
          true
      }

      try {
        task.get(1, TimeUnit.MILLISECONDS)
      } catch {
        case e =>
          taskExecutionLatch(0).countDown()
      }

      taskExecutionLatch(0).await(2000, TimeUnit.MILLISECONDS) must be equalTo true
    }

    "be cancelled after the timeout" in {
      taskExecutionLatch(1).setCount(1)

      val task = distributedTask {
        () =>
          Thread.sleep(1000)
          taskExecutionLatch(1).countDown()
          true
      }

      try {
        task.get(1, TimeUnit.MILLISECONDS)
      } catch {
        case e =>
          task.cancel()
      }

      taskExecutionLatch(1).await(2000, TimeUnit.MILLISECONDS) must be equalTo false
    }

    "be cancelled with all the running predecessors" in {
      taskExecutionLatch(2).setCount(2)

      val task = distributedTask {
        () =>
          Thread.sleep(1000)
          taskExecutionLatch(2).countDown()
          1
      } map {
        x =>
          taskExecutionLatch(2).countDown()
          x + 1
      }

      try {
        task.get(1, TimeUnit.MILLISECONDS)
      } catch {
        case e =>
          task.cancelAll()
      }

      taskExecutionLatch(2).await(2000, TimeUnit.MILLISECONDS) must be equalTo false
    }
  }

  "Long-running multi-task cancellation" in additionalInstance {
    taskExecutionLatch(3).setCount(1)

    val membersCount = HazelcastUtil.clusterMembers.size

    val mtask = multiTask(HazelcastUtil.clusterMembers) {
      () =>
        Thread.sleep(1000)
        taskExecutionLatch(3).countDown()
        1
    }.map(_.sum)

    try {
      mtask.get(1, TimeUnit.MILLISECONDS)
    } catch {
      case e => mtask.cancelAll()
    }

    taskExecutionLatch(3).await(2000, TimeUnit.MILLISECONDS) must be equalTo false

  }

  step {
    TestSuite.stopTest
  }

}