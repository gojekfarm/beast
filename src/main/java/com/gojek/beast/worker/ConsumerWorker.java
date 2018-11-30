package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.Status;

public class ConsumerWorker implements Worker {
    private final MessageConsumer messageConsumer;
    private volatile boolean stop;

    public ConsumerWorker(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        do {
            Status consume = messageConsumer.consume();
        } while (!stop);
    }

    @Override
    public void stop() {
        this.stop = true;
    }
}
