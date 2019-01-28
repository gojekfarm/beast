package com.gojek.beast.worker;

import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQueryException;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class BqQueueWorker extends Worker {
    // Should have separate instance of sink for this worker
    private final Sink sink;
    private final QueueConfig config;
    private final BlockingQueue<Records> queue;
    private final Acknowledger acknowledger;
    private final Stats statsClient = Stats.client();
    private volatile boolean stop;

    public BqQueueWorker(BlockingQueue<Records> queue, Sink sink, QueueConfig config, Acknowledger acknowledger) {
        this.queue = queue;
        this.sink = sink;
        this.config = config;
        this.acknowledger = acknowledger;
    }

    @Override
    public Status job() {
        Instant start = Instant.now();
        try {
            Records poll = queue.poll(config.getTimeout(), config.getTimeoutUnit());
            if (poll == null || poll.isEmpty()) return new SuccessStatus();
            boolean success = pushToSink(poll);
            if (!success) {
                queue.offer(poll, config.getTimeout(), config.getTimeoutUnit());
                return new FailureStatus(new RuntimeException("Push failed"));
            }
        } catch (InterruptedException | RuntimeException e) {
            statsClient.increment("worker.queue.bq.errors");
            log.debug("Exception::Failed to poll records from read queue: " + e.getMessage());
        }
        statsClient.timeIt("worker.queue.bq.processing", start);
        return new SuccessStatus();
    }

    private boolean pushToSink(Records poll) {
        Status status;
        try {
            status = sink.push(poll);
        } catch (BigQueryException e) {
            statsClient.increment("worker.queue.bq.errors");
            log.error("Exception::Failed to write to BQ: {}", e.getMessage());
            return false;
        }
        if (status.isSuccess()) {
            return acknowledger.acknowledge(poll.getPartitionsCommitOffset());
        } else {
            statsClient.increment("worker.queue.bq.push_failure");
            log.error("Failed to push records to sink {}", status.toString());
        }
        return false;
    }

    @Override
    public void stop(String reason) {
        if (stop) return;
        log.debug("Stopping BqWorker: {}", reason);
        sink.close(reason);
        stop = true;
    }
}
