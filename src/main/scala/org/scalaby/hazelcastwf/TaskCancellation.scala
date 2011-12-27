package org.scalaby.hazelcastwf

import com.hazelcast.core.{Hazelcast, MessageListener, DistributedTask => HazelcastDistributedTask}


/**
 * User: remeniuk
 */

sealed trait TaskExecutionMessage

case object RemoveTaskListener extends TaskExecutionMessage

case object CancelTask extends TaskExecutionMessage

object TaskCancellation {

  val TASK_CHANNEL_PREFIX = "task-channel:"

  private def taskTopic(id: String) = Hazelcast.getTopic[TaskExecutionMessage](TASK_CHANNEL_PREFIX + id)

  def removeListener(id: String) = sendMessageToTaskListener(id, RemoveTaskListener)

  def cancel(id: String) = {
    sendMessageToTaskListener(id, CancelTask)
  }

  private def sendMessageToTaskListener(id: String, message: TaskExecutionMessage) =
    taskTopic(id).publish(CancelTask)

}

trait TaskCancellation extends MessageListener[TaskExecutionMessage] {
  self: HazelcastDistributedTask[_] =>

  import TaskCancellation._

  lazy val topicId: String = ""

  taskTopic(topicId).addMessageListener(this)

  private def removeTaskListener = taskTopic(topicId).removeMessageListener(this)

  def onMessage(message: TaskExecutionMessage) = message match {
    case RemoveTaskListener =>
      removeTaskListener
    case CancelTask =>
      this.cancel(true)
      removeTaskListener
  }

}