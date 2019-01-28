package com.gojek.beast.commiter;

import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.worker.CoolWorker;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OffsetCommitter extends CoolWorker implements Committer {
    private static final int DEFAULT_SLEEP_MS = 100;
    private final Stats statsClient = Stats.client();
    private BlockingQueue<Records> commitQueue;
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck;
    private KafkaCommitter kafkaCommitter;
    @Setter
    private long defaultSleepMs;


    private OffsetState offsetState;

    public OffsetCommitter(BlockingQueue<Records> commitQueue, Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck, KafkaCommitter kafkaCommitter, OffsetState offsetState) {
        this.commitQueue = commitQueue;
        this.partitionOffsetAck = partitionOffsetAck;
        this.kafkaCommitter = kafkaCommitter;
        this.defaultSleepMs = DEFAULT_SLEEP_MS;
        this.offsetState = offsetState;
    }

    @Override
    public void close(String reason) {
        log.info("Closing committer: {}", reason);
        kafkaCommitter.wakeup(reason);
    }

    @Override
    public void stop(String reason) {
        log.info("Closing committer: {}", reason);
        kafkaCommitter.wakeup(reason);
    }

    @Override
    public boolean acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets) {
        boolean status = partitionOffsetAck.add(offsets);
        statsClient.gauge("queue.elements,name=ack", partitionOffsetAck.size());
        return status;
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
                sleep(defaultSleepMs);
                statsClient.gauge("committer.queue.wait.ms", defaultSleepMs);
            }
            statsClient.timeIt("committer.processing.time", start);
        } catch (InterruptedException | RuntimeException e) {
            return new FailureStatus(new RuntimeException("Exception in offset committer: " + e.getMessage()));
        } finally {
            log.info("Stopped Offset Committer Successfully.");
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
