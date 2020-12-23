package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerWorker extends Worker {
    private final MessageConsumer messageConsumer;

    public ConsumerWorker(String name, MessageConsumer messageConsumer, WorkerState workerState) {
        super(name, workerState);
        this.messageConsumer = messageConsumer;
    }

    @Override
    public Status job() {
        // always send success since this method is only a placeholder
        return new SuccessStatus();
    }

    @Override
    protected void job2() throws InvalidProtocolBufferException {
        messageConsumer.consume();
    }

    @Override
    public void stop(String reason) {
        log.info("Stopping consumer worker with reason {}", reason);
        messageConsumer.close();
    }
}
