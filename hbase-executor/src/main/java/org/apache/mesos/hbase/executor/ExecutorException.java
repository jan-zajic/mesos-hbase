package org.apache.mesos.hbase.executor;

/**
 * A invalid condition exist within the executor.
 */
public class ExecutorException extends RuntimeException {

  public ExecutorException(String message) {
    super(message);
  }

  public ExecutorException(Throwable cause) {
    super(cause);
  }

  public ExecutorException(String message, Throwable cause) {
    super(message, cause);
  }

}
