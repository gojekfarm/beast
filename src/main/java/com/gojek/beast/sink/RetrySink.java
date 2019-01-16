package com.gojek.beast.sink;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.executor.RetryExecutor;
import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
public class RetrySink implements Sink {
    private final Stats statsClient = Stats.client();
    private Sink sink;
    private BackOffProvider backOffProvider;
    private int maxRetryAttempts;

    @Override
    public Status push(Records records) {
        Instant start = Instant.now();
        Status pushStatus;
        int attemptCount = 0;

        RetryExecutor retryExecutor = new RetryExecutor(sink, records);
        do {
            attemptCount++;
            pushStatus = retryExecutor.execute().ifFailure(backOffProvider, attemptCount).status();
        } while ((attemptCount < maxRetryAttempts) && (!pushStatus.isSuccess()));

        statsClient.gauge("RetrySink.queue.push.messages", records.size());
        statsClient.timeIt("RetrySink.queue.push.time", start);
        return pushStatus;
    }

    @Override
    public void close() {
        sink.close();
    }
}
