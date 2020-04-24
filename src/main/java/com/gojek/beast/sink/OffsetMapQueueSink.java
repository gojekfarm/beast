package com.gojek.beast.sink;

import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
public class OffsetMapQueueSink implements Sink {
    private final Stats statsClient = Stats.client();
    private final BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> recordQueue;
    private final QueueConfig config;

    @Override
    public Status push(Records records) {
        Instant start = Instant.now();
        boolean offered;
        Map<TopicPartition, OffsetAndMetadata> offsetmap = records.getPartitionsCommitOffset();
        try {
            offered = recordQueue.offer(offsetmap, config.getTimeout(), config.getTimeoutUnit());
            statsClient.gauge("queue.elements,name=" + config.getName(), recordQueue.size());
        } catch (InterruptedException e) {
            return new FailureStatus(e);
        }
        statsClient.count("sink.queue.push.messages", offsetmap.size());
        statsClient.timeIt("sink.queue.push.time", start);
        return offered ? new SuccessStatus()
                : new FailureStatus(new RuntimeException(String.format("%s queue is full with capacity: %d", config.getName(), recordQueue.size())));

    }


    @Override
    public void close(String reason) {

    }
}
