package com.gojek.beast.sink.executor;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.SinkElement;
import com.gojek.beast.stats.Stats;

public class RetryExecutor implements Executor {

    private Sink sink;
    private SinkElement records;
    private int maxAttempts;
    private BackOffProvider backOffProvider;
    private final Stats statsClient = Stats.client(); // metrics client

    private Status status;
    private int attemptCount = 0;

    public RetryExecutor(Sink sink, SinkElement records, int maxAttempts, BackOffProvider backOffProvider) {
        this.sink = sink;
        this.records = records;
        this.maxAttempts = maxAttempts;
        this.backOffProvider = backOffProvider;
    }

    @Override
    public Status status() {
        statsClient.gauge("RetrySink.queue.push.attempts", attemptCount);
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
            statsClient.increment("retrysink.exec.failure.count," + statsClient.getBqTags());
            status = new FailureStatus(e);
        }
        ifFailure();
        return this;
    }
}
