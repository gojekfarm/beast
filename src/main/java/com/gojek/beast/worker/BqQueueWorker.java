package com.gojek.beast.worker;

import com.gojek.beast.commiter.Committer;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQueryException;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class BqQueueWorker implements Worker {
    // Should have separate instance of sink for this worker
    private final Sink sink;
    private final QueueConfig config;
    private final BlockingQueue<Records> queue;
    private final Committer committer;
    private final Stats statsClient = Stats.client();
    private volatile boolean stop;

    public BqQueueWorker(BlockingQueue<Records> queue, Sink sink, QueueConfig config, Committer committer) {
        this.queue = queue;
        this.sink = sink;
        this.config = config;
        this.committer = committer;
    }

    @Override
    public void run() {
        String failureReason;
        do {
            Instant start = Instant.now();
            try {
                Records poll = queue.poll(config.getTimeout(), config.getTimeoutUnit());
                if (poll == null || poll.isEmpty()) {
                    continue;
                }
                boolean success = pushToSink(poll);
                if (!success) {
                    queue.offer(poll, config.getTimeout(), config.getTimeoutUnit());
                    failureReason = "Error::Failed to push records to sink";
                    log.error(failureReason);
                    stop(failureReason);
                }
            } catch (InterruptedException | RuntimeException e) {
                statsClient.increment("worker.queue.bq.errors");
                failureReason = "Exception::Failed to poll records from read queue: " + e.getMessage();
                log.error(failureReason);
                stop(failureReason);
            }
            statsClient.timeIt("worker.queue.bq.processing", start);
        } while (!stop);
        log.info("Stopped BQ Queue Worker Successfully.");
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
            return committer.acknowledge(poll.getPartitionsCommitOffset());
        } else {
            statsClient.increment("worker.queue.bq.push_failure");
        }
        return false;
    }

    @Override
    public void stop(String reason) {
        log.info("Stopping BqWorker: {}", reason);
        committer.close(reason);
        sink.close(reason);
        stop = true;
    }
}
