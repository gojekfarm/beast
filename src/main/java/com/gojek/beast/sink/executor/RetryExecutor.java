package com.gojek.beast.sink.executor;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;

public class RetryExecutor implements Executor {

    private Sink sink;
    private Records records;

    private Status status;

    public RetryExecutor(Sink sink, Records records) {
        this.sink = sink;
        this.records = records;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public Executor ifFailure(BackOffProvider backOffProvider, int attemptCount) {
        if (!status.isSuccess())
            backOffProvider.backOff(attemptCount);
        return this;
    }

    @Override
    public Executor execute() {
        try {
            status = sink.push(records);
        } catch (Exception e) {
            status = new FailureStatus(e);
        }
        return this;
    }
}
