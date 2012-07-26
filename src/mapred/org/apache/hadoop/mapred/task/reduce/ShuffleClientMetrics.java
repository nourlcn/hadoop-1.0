package org.apache.hadoop.mapred.task.reduce;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;

public class ShuffleClientMetrics implements Updater
{

    private MetricsRecord shuffleMetrics = null;
    private int numFailedFetches = 0;
    private int numSuccessFetches = 0;
    private long numBytes = 0;
    private int numThreadsBusy = 0;
    private final int numCopiers;

    ShuffleClientMetrics(TaskAttemptID reduceId, JobConf jobConf)
    {
        this.numCopiers = jobConf.getInt("mapred.reduce.parallel.copies", 5);

        MetricsContext metricsContext = MetricsUtil.getContext("mapred");
        this.shuffleMetrics = MetricsUtil.createRecord(metricsContext, "shuffleInput");
        this.shuffleMetrics.setTag("user", jobConf.getUser());
        this.shuffleMetrics.setTag("jobName", jobConf.getJobName());
        this.shuffleMetrics.setTag("jobId", reduceId.getJobID().toString());
        this.shuffleMetrics.setTag("taskId", reduceId.toString());
        this.shuffleMetrics.setTag("sessionId", jobConf.getSessionId());
        metricsContext.registerUpdater(this);
    }

    public synchronized void inputBytes(long numBytes)
    {
        this.numBytes += numBytes;
    }

    public synchronized void failedFetch()
    {
        ++numFailedFetches;
    }

    public synchronized void successFetch()
    {
        ++numSuccessFetches;
    }

    public synchronized void threadBusy()
    {
        ++numThreadsBusy;
    }

    public synchronized void threadFree()
    {
        --numThreadsBusy;
    }

    public void doUpdates(MetricsContext unused)
    {
        synchronized (this)
        {
            shuffleMetrics.incrMetric("shuffle_input_bytes", numBytes);
            shuffleMetrics.incrMetric("shuffle_failed_fetches", numFailedFetches);
            shuffleMetrics.incrMetric("shuffle_success_fetches", numSuccessFetches);
            if (numCopiers != 0)
            {
                shuffleMetrics.setMetric("shuffle_fetchers_busy_percent",
                        100 * ((float) numThreadsBusy / numCopiers));
            }
            else
            {
                shuffleMetrics.setMetric("shuffle_fetchers_busy_percent", 0);
            }
            numBytes = 0;
            numSuccessFetches = 0;
            numFailedFetches = 0;
        }
        shuffleMetrics.update();
    }
}
