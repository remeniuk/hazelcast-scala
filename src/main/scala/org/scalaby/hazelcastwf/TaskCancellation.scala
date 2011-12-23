package org.scalaby.hazelcastwf

import com.hazelcast.core.{Hazelcast, MessageListener, DistributedTask => HazelcastDistributedTask}


/**
 * User: remeniuk
 */

object TaskCancellation {

  val TASK_CHANNEL_PREFIX = "task-channel:"

  private def taskTopic(id: String) = Hazelcast.getTopic[TaskExecutionMessage](TASK_CHANNEL_PREFIX + id)

  def cancel(id: String) = taskTopic(id).publish(CancelTask)

}

trait TaskCancellation extends MessageListener[TaskExecutionMessage] {
  self: HazelcastDistributedTask[_] =>

  import TaskCancellation._

  val topicId: String

  taskTopic(topicId).addMessageListener(this)

  def onMessage(message: TaskExecutionMessage) = message match {
    case CancelTask =>
      this.cancel(true)
      taskTopic(topicId).removeMessageListener(this)
  }

}