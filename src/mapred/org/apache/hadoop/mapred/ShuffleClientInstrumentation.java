package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

public class ShuffleClientInstrumentation implements MetricsSource {
  final MetricsRegistry registry = new MetricsRegistry("shuffleInput");
  final MetricMutableCounterLong inputBytes = registry.newCounter(
      "shuffle_input_bytes", "", 0L);
  final MetricMutableCounterInt failedFetches = registry.newCounter(
      "shuffle_failed_fetches", "", 0);
  final MetricMutableCounterInt successFetches = registry.newCounter(
      "shuffle_success_fetches", "", 0);
  private volatile int threadsBusy = 0;
  private JobConf jobConf;

  @SuppressWarnings("deprecation")
  public ShuffleClientInstrumentation(JobConf conf, String reduceTaskIdString,
      TaskID taskId) {
    this.jobConf = conf;
    registry.tag("user", "User name", conf.getUser())
        .tag("jobName", "Job name", conf.getJobName())
        // .tag("jobId", "Job ID", ReduceTask.this.getJobID().toString())
        .tag("jobId", "Job ID", reduceTaskIdString)
        // .tag("taskId", "Task ID", getTaskID().toString())
        .tag("taskId", "Task ID", taskId.toString())
        .tag("sessionId", "Session ID", conf.getSessionId());
  }

  // @Override
  public void inputBytes(long numBytes) {
    inputBytes.incr(numBytes);
  }

  // @Override
  public void failedFetch() {
    failedFetches.incr();
  }

  // @Override
  public void successFetch() {
    successFetches.incr();
  }

  // @Override
  public synchronized void threadBusy() {
    ++threadsBusy;
  }

  // @Override
  public synchronized void threadFree() {
    --threadsBusy;
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    int numCopiers = this.jobConf.getInt("mapred.reduce.parallel.copies", 5);
    ;
    MetricsRecordBuilder rb = builder.addRecord(registry.name());
    rb.addGauge("shuffle_fetchers_busy_percent", "", numCopiers == 0 ? 0 : 100.
        * threadsBusy / numCopiers);
    registry.snapshot(rb, all);
  }

}