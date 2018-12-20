package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.WakeupException;

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
            try {
                Status status = messageConsumer.consume();
                if (!status.isSuccess()) {
                    log.error("message consumption failed: {}", status.toString());
                    statsClient.increment("worker.consumer.consume.errors");
                }
            } catch (WakeupException e) {
                log.error("Stop Message Consumption: {}", e);
                messageConsumer.close();
                stop = true;
            }
        } while (!stop);
    }

    @Override
    public void stop() {
        log.info("Stopping consumer");
        stop = true;
    }
}
