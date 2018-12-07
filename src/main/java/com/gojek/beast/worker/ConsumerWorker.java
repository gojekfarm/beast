package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.Status;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerWorker implements Worker {
    private final MessageConsumer messageConsumer;
    private volatile boolean stop;

    public ConsumerWorker(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        do {
            // TODO: Check if should be `synchronized`
            synchronized (messageConsumer) {
                Status status = messageConsumer.consume();
                if (!status.isSuccess()) {
                    log.error(status.toString());
                }
            }
        } while (!stop);
    }

    @Override
    public void stop() {
        stop = true;
    }
}
