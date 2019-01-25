package com.gojek.beast.commiter;

import com.gojek.beast.models.Records;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.worker.StopEvent;
import com.gojek.beast.worker.Worker;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.Subscribe;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OffsetCommitter implements Committer, Worker {
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
        EventBus.getDefault().register(this);
    }

    @Override
    public void close(String reason) {
        log.info("Closing committer: {}", reason);
        stop = true;
        kafkaCommitter.wakeup(reason);
    }

    @Override
    public boolean acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets) {
        boolean status = partitionOffsetAck.add(offsets);
        statsClient.gauge("queue.elements,name=ack", partitionOffsetAck.size());
        return status;
    }

    @Override
    public void run() {
        String failureReason = "UNKNOWN";
        offsetState.startTimer();
        try {
            while (!stop) {
                Instant start = Instant.now();
                Records commitOffset = commitQueue.peek();
                if (commitOffset == null) {
                    continue;
                }
                Map<TopicPartition, OffsetAndMetadata> currentOffset = commitOffset.getPartitionsCommitOffset();
                if (partitionOffsetAck.contains(currentOffset)) {
                    Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = commitQueue.remove().getPartitionsCommitOffset();
                    kafkaCommitter.commitSync(partitionsCommitOffset);
                    partitionOffsetAck.remove(partitionsCommitOffset);
                    Records nextOffset = commitQueue.peek();
                    if (nextOffset != null) offsetState.resetOffset(nextOffset.getPartitionsCommitOffset());
                    log.info("commit partition {} size {}", partitionsCommitOffset.toString(), partitionsCommitOffset.size());
                } else {
                    if (offsetState.shouldCloseConsumer(currentOffset)) {
                        failureReason = "Acknowledgement Timeout exceeded: " + offsetState.getAcknowledgeTimeoutMs();
                        statsClient.increment("committer.ack.timeout");
                        close(failureReason);
                    }
                    Thread.sleep(defaultSleepMs);
                    statsClient.gauge("committer.queue.wait.ms", defaultSleepMs);
                }
                statsClient.timeIt("committer.processing.time", start);
            }
        } catch (InterruptedException | RuntimeException e) {
            failureReason = "Exception in offset committer: " + e.getMessage();
        } finally {
            close(failureReason);
        }
        log.info("Stopped Offset Committer Successfully.");
    }

    @Override
    public void stop(String reason) {
        close(reason);
        stop = true;
        EventBus.getDefault().unregister(this);
    }

    @Subscribe
    public void onCloseEvent(StopEvent event) {
        log.debug("Got Event and closed commiter {} ", event);
        stop(event.getReason());
    }
}

