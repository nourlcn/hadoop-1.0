package org.apache.hadoop.mapred.task.reduce;

public interface ExceptionReporter {
  void reportException(Throwable t);
}
