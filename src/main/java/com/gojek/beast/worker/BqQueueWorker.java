package com.gojek.beast.worker;

import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.exception.ErrorWriterFailedException;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQueryException;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;

import static com.gojek.beast.config.Constants.SUCCESS_STATUS;

@Slf4j
public class BqQueueWorker extends Worker {
    // Should have separate instance of sink for this worker
    private final Sink sink;
    private final QueueConfig config;
    private final BlockingQueue<Records> queue;
    private final Acknowledger acknowledger;
    private final Stats statsClient = Stats.client();

    public BqQueueWorker(String name, Sink sink, QueueConfig config, Acknowledger acknowledger, BlockingQueue<Records> queue, WorkerState workerState) {
        super(name, workerState);
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
            if (poll == null || poll.isEmpty()) return SUCCESS_STATUS;
            Status status = pushToSink(poll);
            if (!status.isSuccess()) {
                queue.offer(poll, config.getTimeout(), config.getTimeoutUnit());
                return status;
            }
        } catch (InterruptedException | RuntimeException e) {
            statsClient.increment("worker.queue.bq.errors");
            log.debug("Exception::Failed to poll records from read queue: " + e.getMessage());
            return new FailureStatus(e);
        }
        statsClient.timeIt("worker.queue.bq.processing.time", start);
        return SUCCESS_STATUS;
    }

    private Status pushToSink(Records poll) {
        Status status;
        try {
            status = sink.push(poll);
            statsClient.count("kafka.batch.records.size," + statsClient.getBqTags(), poll.getSize());
            poll.getRecordCountByPartition().forEach((partition, recordCount) -> statsClient.count("kafka.batch.records.count," + statsClient.getBqTags() + ",partition=" + partition.toString(), recordCount));
        } catch (BigQueryException e) {
            statsClient.increment("worker.queue.bq.errors");
            log.error("Exception::Failed to write to BQ: {}", e.getMessage());
            return new FailureStatus(e);
        } catch (ErrorWriterFailedException bqhe) {
            statsClient.increment("worker.queue.handler.errors");
            log.error("Exception::Could not process the errors with handler sink: {}", bqhe.getMessage());
            return new FailureStatus(bqhe);
        }
        if (status.isSuccess()) {
            boolean ackStatus = acknowledger.acknowledge(poll.getPartitionsCommitOffset());
            statsClient.timeIt("batch.processing.latency.time," + statsClient.getBqTags(), poll.getPolledTime());
            if (!ackStatus) {
                statsClient.increment("batch.partition.offsets.reprocessed");
            }
            return SUCCESS_STATUS;
        } else {
            statsClient.increment("worker.queue.bq.push_failure");
            log.error("Failed to push records to sink {}", status.toString());
            return status;
        }
    }

    @Override
    public void stop(String reason) {
        log.info("Stopping BqWorker: {}", reason);
        sink.close(reason);
    }
}
