package com.gojek.beast.commiter;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.worker.Worker;
import lombok.Setter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class OffsetCommitter implements Sink, Committer, Worker {
    private static final int DEFAULT_SLEEP_MS = 100;
    private Queue<Records> commitQueue;
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck;
    private KafkaConsumer<byte[], byte[]> consumer;
    @Setter
    private long defaultSleepMs;

    private volatile boolean stop;

    public OffsetCommitter(Queue<Records> commitQueue, Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck, KafkaConsumer<byte[], byte[]> consumer) {
        this.commitQueue = commitQueue;
        this.partitionOffsetAck = partitionOffsetAck;
        this.consumer = consumer;
        this.defaultSleepMs = DEFAULT_SLEEP_MS;
    }

    public Status push(Records records) {
        commitQueue.add(records);
        return new SuccessStatus();
    }

    @Override
    public void close() {
    }

    @Override
    public void acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets) {
        partitionOffsetAck.add(offsets);
    }

    @Override
    public void run() {
        while (!stop) {
            Records commitOffset = commitQueue.peek();
            if (commitOffset != null && partitionOffsetAck.contains(commitOffset.getPartitionsCommitOffset())) {
                Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = commitQueue.remove().getPartitionsCommitOffset();
                consumer.commitSync(partitionsCommitOffset);
                partitionOffsetAck.remove(partitionsCommitOffset);
            } else {
                try {
                    Thread.sleep(defaultSleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void stop() {
        stop = true;
    }
}
