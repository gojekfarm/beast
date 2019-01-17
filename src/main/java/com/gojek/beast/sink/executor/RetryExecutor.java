package com.gojek.beast.sink.executor;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;

public class RetryExecutor implements Executor {

    private Sink sink;
    private Records records;
    private int maxAttempts;
    private BackOffProvider backOffProvider;

    private Status status;
    private int attemptCount = 0;

    public RetryExecutor(Sink sink, Records records, int maxAttempts, BackOffProvider backOffProvider) {
        this.sink = sink;
        this.records = records;
        this.maxAttempts = maxAttempts;
        this.backOffProvider = backOffProvider;
    }

    @Override
    public Status status() {
        return status;
    }

    private boolean shouldExecute() {
        return ((attemptCount < maxAttempts) && (!status.isSuccess()));
    }

    @Override
    public Executor ifFailure() {
        if (shouldExecute()) {
            backOffProvider.backOff(attemptCount);
            execute();
        }
        return this;
    }

    @Override
    public Executor execute() {
        attemptCount++;
        try {
            status = sink.push(records);
        } catch (Exception e) {
            status = new FailureStatus(e);
        }
        ifFailure();
        return this;
    }
}
