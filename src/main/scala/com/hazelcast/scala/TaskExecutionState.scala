package com.hazelcast.scala

/**
 * User: remeniuk
 */

sealed trait TaskExecutionState

sealed trait ExecutionNotStarted extends TaskExecutionState

sealed trait ExecutionStarted extends TaskExecutionState