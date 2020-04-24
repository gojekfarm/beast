package com.gojek.beast.sink;

import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.concurrent.BlockingQueue;

import static com.gojek.beast.config.Constants.SUCCESS_STATUS;

@AllArgsConstructor
public class RecordsQueueSink implements Sink {
    private final Stats statsClient = Stats.client();
    private final BlockingQueue<Records> recordQueue;
    private final QueueConfig config;

    @Override
    public Status push(Records messages) {
        Instant start = Instant.now();
        boolean offered;
        try {
            offered = recordQueue.offer(messages, config.getTimeout(), config.getTimeoutUnit());
            statsClient.gauge("queue.elements,name=" + config.getName(), recordQueue.size());
        } catch (InterruptedException e) {
            return new FailureStatus(e);
        }
        statsClient.count("readQueueSink.push.messages", messages.size());
        statsClient.timeIt("readQueueSink.push.time", start);

        if (offered) {
            return SUCCESS_STATUS;
        } else {
            statsClient.increment("readQueueSink.push.failures");
            return new FailureStatus(new RuntimeException(String.format("%s queue is full with capacity: %d", config.getName(), recordQueue.size())));
        }
    }

    @Override
    public void close(String reason) {
        recordQueue.clear();
    }
}
