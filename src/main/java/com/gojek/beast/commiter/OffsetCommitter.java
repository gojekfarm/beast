package com.gojek.beast.commiter;

import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.worker.Worker;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OffsetCommitter implements Sink, Committer, Worker {
    private static final int DEFAULT_SLEEP_MS = 100;
    private final Stats statsClient = Stats.client();
    private BlockingQueue<Records> commitQueue;
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck;
    private KafkaCommitter kafkaCommitter;
    @Setter
    private long defaultSleepMs;


    private OffsetState offsetState;

    private volatile boolean stop;

    public OffsetCommitter(BlockingQueue<Records> commitQueue, Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck, KafkaCommitter kafkaCommitter, OffsetState offsetState) {
        this.commitQueue = commitQueue;
        this.partitionOffsetAck = partitionOffsetAck;
        this.kafkaCommitter = kafkaCommitter;
        this.defaultSleepMs = DEFAULT_SLEEP_MS;
        this.offsetState = offsetState;
    }

    public Status push(Records records) {
        try {
            commitQueue.put(records);
            statsClient.gauge("queue.elements,name=commiter", commitQueue.size());
        } catch (InterruptedException e) {
            log.error("push to commit queue failed: {}", e.getMessage());
            return new FailureStatus(e);
        }
        return new SuccessStatus();
    }

    @Override
    public void close() {
        log.info("Closing committer");
        kafkaCommitter.wakeup();
        System.exit(1);
    }

    @Override
    public void acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets) {
        partitionOffsetAck.add(offsets);
        statsClient.gauge("queue.elements,name=ack", partitionOffsetAck.size());
    }

    @Override
    public void run() {
        offsetState.startTimer();

        while (!stop) {
            Instant start = Instant.now();
            Records commitOffset = commitQueue.peek();
            if (commitOffset == null) {
                continue;
            }
            Map<TopicPartition, OffsetAndMetadata> currentOffset = commitOffset.getPartitionsCommitOffset();
            if (partitionOffsetAck.contains(currentOffset)) {
                Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = commitQueue.remove().getPartitionsCommitOffset();
                offsetState.resetOffset(partitionsCommitOffset);
                synchronized (kafkaCommitter) {
                    kafkaCommitter.commitSync(partitionsCommitOffset);
                }
                partitionOffsetAck.remove(partitionsCommitOffset);
                log.info("commit partition {} size {}", partitionsCommitOffset.toString(), partitionsCommitOffset.size());
            } else {
                if (offsetState.shouldCloseConsumer(currentOffset)) {
                    log.error("Acknowledgement Timeout exceeded: {}", offsetState.getAcknowledgeTimeoutMs());
                    statsClient.increment("committer.ack.timeout");
                    close();
                }
                try {
                    Thread.sleep(defaultSleepMs);
                    statsClient.gauge("committer.queue.wait.ms", defaultSleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            statsClient.timeIt("committer.processing.time", start);
        }
        kafkaCommitter.wakeup();
    }

    @Override
    public void stop() {
        stop = true;
    }

}

