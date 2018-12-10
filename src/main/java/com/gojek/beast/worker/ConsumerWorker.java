package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerWorker implements Worker {
    private final MessageConsumer messageConsumer;
    private final Stats statsClient = Stats.client();
    private volatile boolean stop;

    public ConsumerWorker(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        do {
            Status status = messageConsumer.consume();
            if (!status.isSuccess()) {
                log.error("message consumption failed: {}", status.toString());
                statsClient.increment("worker.consumer.consume.errors");
            }
        } while (!stop);
    }

    @Override
    public void stop() {
        stop = true;
    }
}
