package org.apache.hadoop.tools.util;


/**
 * <p>
 * Specifies a policy for retrying method failures.
 * Implementations of this interface should be immutable.
 * </p>
 */
public interface RetryPolicy {
  /**
   * <p>
   * Determines whether the framework should retry a
   * method for the given exception, and the number
   * of retries that have been made for that operation
   * so far.
   * </p>
   * @param e The exception that caused the method to fail.
   * @param retries The number of times the method has been retried.
   * @return <code>true</code> if the method should be retried,
   *   <code>false</code> if the method should not be retried
   *   but shouldn't fail with an exception (only for void methods).
   * @throws Exception The re-thrown exception <code>e</code> indicating
   *   that the method failed and should not be retried further.
   */
  public boolean shouldRetry(Exception e, int retries) throws Exception;
}
