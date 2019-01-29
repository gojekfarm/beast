package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerWorker extends Worker {
    private final MessageConsumer messageConsumer;
    private final Stats statsClient = Stats.client();

    public ConsumerWorker(String name, MessageConsumer messageConsumer) {
        super(name);
        this.messageConsumer = messageConsumer;
    }

    @Override
    public Status job() {
        Status status;
        try {
            status = messageConsumer.consume();
            if (!status.isSuccess()) {
                log.error("message consumption failed: {}", status.toString());
                statsClient.increment("worker.consumer.consume.errors");
            }
        } catch (RuntimeException e) {
            log.error("Exception::Stop Message Consumption: {}", e.getMessage());
            return new FailureStatus(e);
        }
        return status;
    }

    @Override
    public void stop(String reason) {
        log.info("Stopping consumer");
        messageConsumer.close();
    }
}
