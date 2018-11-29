package com.gojek.beast.sink;

import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;

import java.util.concurrent.BlockingQueue;

@AllArgsConstructor
public class QueueSink implements Sink<Record> {
    private BlockingQueue<Iterable<Record>> recordQueue;

    @Override
    public Status push(Iterable<Record> messages) {
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
