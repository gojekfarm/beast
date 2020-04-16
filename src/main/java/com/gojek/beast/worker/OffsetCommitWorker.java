package com.gojek.beast.worker;

import com.gojek.beast.Clock;
import com.gojek.beast.commiter.KafkaCommitter;
import com.gojek.beast.commiter.OffsetState;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.OffsetMap;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.OffsetMetadata;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.stats.Stats;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OffsetCommitWorker extends Worker {
    private static final int DEFAULT_SLEEP_MS = 100;
    private final Stats statsClient = Stats.client();
    private final BlockingQueue<OffsetMap> commitQueue;
    private final QueueConfig queueConfig;
    private final KafkaCommitter kafkaCommitter;
    @Setter
    private long defaultSleepMs;
    private boolean stopped;
    private OffsetState offsetState;
    private Clock clock;

    public OffsetCommitWorker(String name, QueueConfig queueConfig, KafkaCommitter kafkaCommitter, OffsetState offsetState, BlockingQueue<OffsetMap> commitQueue, WorkerState workerState, Clock clock) {
        super(name, workerState);
        this.clock = clock;
        this.queueConfig = queueConfig;
        this.commitQueue = commitQueue;
        this.kafkaCommitter = kafkaCommitter;
        this.defaultSleepMs = DEFAULT_SLEEP_MS;
        this.offsetState = offsetState;
        this.stopped = false;
    }

    @Override
    public void stop(String reason) {
        log.info("Closing committer: {}", reason);
        this.stopped = true;
        kafkaCommitter.wakeup(reason);
    }

    @Override
    public Status job() {
        offsetState.startTimer();
        try {
            Instant startTime = Instant.now();
            long start = clock.currentEpochMillis();
            Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = new HashMap<>();

            int offsetClubbedBatches = 0;
            while (true) {
                OffsetMap currentOffset = commitQueue.poll(queueConfig.getTimeout(), queueConfig.getTimeoutUnit());
                if (stopped || clock.currentEpochMillis() - start > offsetState.getOffsetCommitTime()) {
                    break;
                }

                if (currentOffset == null) {
                    continue;
                }

                Instant commitQueuePollStartTime = Instant.now();
                while (true) {
                    if (offsetState.removeFromOffsetAck(currentOffset)) {
                        commitQueue.remove(currentOffset);

                        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = currentOffset.getOffsetAndMetadataMap();
                        offsetAndMetadataMap.keySet().forEach(topicPartition -> {
                            OffsetAndMetadata offsetAndMetadata = offsetAndMetadataMap.get(topicPartition);
                            OffsetMetadata previousOffset = (OffsetMetadata) partitionsCommitOffset.getOrDefault(topicPartition, new OffsetMetadata(Integer.MIN_VALUE));
                            OffsetMetadata newOffset = new OffsetMetadata(offsetAndMetadata.offset());
                            if (previousOffset.compareTo(newOffset) < 0) {
                                partitionsCommitOffset.put(topicPartition, newOffset);
                            }
                        });
                        offsetClubbedBatches++;
                        break;
                    } else {
                        if (offsetState.shouldCloseConsumer(partitionsCommitOffset)) {
                            statsClient.increment("committer.ack.timeout");
                            return new FailureStatus(new RuntimeException("Acknowledgement Timeout exceeded: " + offsetState.getAcknowledgeTimeoutMs()));
                        }

                        log.debug("waiting for {} acknowledgement for offset {}, {}, {}", defaultSleepMs, currentOffset, offsetState.partitionOffsetAckSize(), currentOffset);
                        sleep(defaultSleepMs);
                    }
                }
                statsClient.timeIt("committer.queue.wait.ms", commitQueuePollStartTime);
            }
            commit(partitionsCommitOffset);

            statsClient.gauge("committer.clubbed.offsets", offsetClubbedBatches);
            statsClient.timeIt("committer.processing.time", startTime);
        } catch (InterruptedException | RuntimeException e) {
            log.info("Received {} exception: {}, resetting committer", e.getClass(), e.getMessage());
            e.printStackTrace();
            return new FailureStatus(new RuntimeException("Exception in offset committer: " + e.getMessage()));
        }
        return new SuccessStatus();
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset) {
        if (partitionsCommitOffset.size() != 0) {
            kafkaCommitter.commitSync(partitionsCommitOffset);
            offsetState.resetOffset();
        }
        log.info("committed offsets partition {} size {}", partitionsCommitOffset.toString(), partitionsCommitOffset.size());
    }
}
