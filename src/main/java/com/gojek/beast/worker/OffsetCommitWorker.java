package com.gojek.beast.worker;

import com.gojek.beast.commiter.KafkaCommitter;
import com.gojek.beast.commiter.OffsetState;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.stats.Stats;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OffsetCommitWorker extends Worker {
    private static final int DEFAULT_SLEEP_MS = 100;
    private final Stats statsClient = Stats.client();
    private BlockingQueue<Records> commitQueue;
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck;
    private KafkaCommitter kafkaCommitter;
    @Setter
    private long defaultSleepMs;


    private OffsetState offsetState;

    public OffsetCommitWorker(String name, Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck, KafkaCommitter kafkaCommitter, OffsetState offsetState, BlockingQueue<Records> commitQueue, WorkerState workerState) {
        super(name, workerState);
        this.commitQueue = commitQueue;
        this.partitionOffsetAck = partitionOffsetAck;
        this.kafkaCommitter = kafkaCommitter;
        this.defaultSleepMs = DEFAULT_SLEEP_MS;
        this.offsetState = offsetState;
    }

    @Override
    public void stop(String reason) {
        log.info("Closing committer: {}", reason);
        kafkaCommitter.wakeup(reason);
    }

    @Override
    public Status job() {
        offsetState.startTimer();
        try {
            Instant start = Instant.now();
            Records commitOffset = commitQueue.peek();
            if (commitOffset == null) {
                return new SuccessStatus();
            }
            Map<TopicPartition, OffsetAndMetadata> currentOffset = commitOffset.getPartitionsCommitOffset();
            if (partitionOffsetAck.contains(currentOffset)) {
                commit();
            } else {
                if (offsetState.shouldCloseConsumer(currentOffset)) {
                    statsClient.increment("committer.ack.timeout");
                    return new FailureStatus(new RuntimeException("Acknowledgement Timeout exceeded: " + offsetState.getAcknowledgeTimeoutMs()));
                }
                log.debug("waiting for {} acknowledgement for offset {}", defaultSleepMs, currentOffset);
                sleep(defaultSleepMs);
                statsClient.gauge("committer.queue.wait.ms", defaultSleepMs);
            }
            statsClient.timeIt("committer.processing.time", start);
        } catch (InterruptedException | RuntimeException e) {
            return new FailureStatus(new RuntimeException("Exception in offset committer: " + e.getMessage()));
        }
        return new SuccessStatus();
    }

    private void commit() {
        Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = commitQueue.remove().getPartitionsCommitOffset();
        kafkaCommitter.commitSync(partitionsCommitOffset);
        partitionOffsetAck.remove(partitionsCommitOffset);
        Records nextOffset = commitQueue.peek();
        if (nextOffset != null) offsetState.resetOffset(nextOffset.getPartitionsCommitOffset());
        log.info("commit partition {} size {}", partitionsCommitOffset.toString(), partitionsCommitOffset.size());
    }
}
