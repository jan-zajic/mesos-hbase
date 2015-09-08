package org.apache.mesos.hbase.state;

/**
 * Defines node types.
 */
public enum AcquisitionPhase {

  /**
   * Waits here for the timeout on (re)registration.
   */
  /**
  * Waits here for the timeout on (re)registration.
  */
  /**
  * Waits here for the timeout on (re)registration.
  */
  /**
  * Waits here for the timeout on (re)registration.
  */
  RECONCILING_TASKS,

  /**
   * Launches the both namenodes.
   */
  START_MASTER_NODES,

  /**
   * If everything is healthy the scheduler stays here and tries to launch
   * datanodes on any slave that doesn't have an hbase task running on it.
   */
  SLAVE_NODES
}
