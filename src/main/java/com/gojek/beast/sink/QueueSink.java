package com.gojek.beast.sink;

import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import lombok.AllArgsConstructor;

import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
public class QueueSink implements Sink {
    private BlockingQueue<Records> recordQueue;

    @Override
    public Status push(Records messages) {
        try {
            recordQueue.put(messages);
        } catch (InterruptedException e) {
            return new FailureStatus(e);
        }
        return new SuccessStatus();
    }

    @Override
    public void close() {

    }
}
