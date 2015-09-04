package org.apache.mesos.hbase.scheduler;

/**
 * Exceptions in the scheduler which likely result in the scheduler being shutdown.
 */
public class SchedulerException extends RuntimeException {

  public SchedulerException(Throwable cause) {
    super(cause);
  }

  public SchedulerException(String message) {
    super(message);
  }

  public SchedulerException(String message, Throwable cause) {
    super(message, cause);
  }
}
