package org.scalaby.hazelcastwf

import com.hazelcast.core.Hazelcast

/**
 * User: remeniuk
 */

object Promises {

  final val PROMISES_REPOSITORY = "wf-promises-repository"
  final val PROMISED_VALUES_REPOSITORY = "wf-promised-values-repository"

  def repository[V] = Hazelcast.getMap[String, Promise[V]](PROMISES_REPOSITORY)

  def values = Hazelcast.getMap[String, Any](PROMISED_VALUES_REPOSITORY)

  def putResult[T](key: String, value: T) = values.put(key, value)

  def getResult[T](key: String) = values.get(key).asInstanceOf[T]

  def put[T](id: String, promise: Promise[T]) = repository.put(id, promise)

  def get[T](id: String) = repository[T].get(id)

}