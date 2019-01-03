package com.gojek.beast.worker;

import com.gojek.beast.commiter.Committer;
import com.gojek.beast.config.WorkerConfig;
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
    private final WorkerConfig config;
    private final BlockingQueue<Records> queue;
    private final Committer committer;
    private final Stats statsClient = Stats.client();
    private volatile boolean stop;

    public BqQueueWorker(BlockingQueue<Records> queue, Sink sink, WorkerConfig config, Committer committer) {
        this.queue = queue;
        this.sink = sink;
        this.config = config;
        this.committer = committer;
    }

    @Override
    public void run() {
        do {
            Instant start = Instant.now();
            try {
                Records poll = queue.poll(config.getTimeout(), config.getTimeoutUnit());
                if (poll == null || poll.isEmpty()) {
                    continue;
                }
                Status status = sink.push(poll);
                if (status.isSuccess()) {
                    committer.acknowledge(poll.getPartitionsCommitOffset());
                } else {
                    // TODO: retry message when failed, maintain the retry count in `Records`
                    statsClient.increment("worker.queue.bq.push_failure");
                }
            } catch (BigQueryException e) {
                statsClient.increment("worker.queue.bq.errors");
                log.error("Failed to write to BQ: {}", e);
                // This stops the consumer
                committer.close();
                stop();
            } catch (InterruptedException e) {
                statsClient.increment("worker.queue.bq.errors");
                log.error("Failed to poll records from read queue: {}", e);
            }
            statsClient.timeIt("worker.queue.bq.processing", start);
        } while (!stop);
    }

    @Override
    public void stop() {
        log.info("Stopping BqWorker");
        stop = true;
        sink.close();
    }
}
