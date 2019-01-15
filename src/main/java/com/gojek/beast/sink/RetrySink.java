package com.gojek.beast.sink;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
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
        int attemptCount = 0;
        Status pushStatus;
        do {
            attemptCount++;
            try {
                pushStatus = sink.push(records);
            } catch (Exception e) {
                pushStatus = new FailureStatus(e);
            }

            if (pushStatus.isSuccess()) {
                break;
            } else {
                backOffProvider.backOff(attemptCount);
            }
        } while (attemptCount < maxRetryAttempts);

        statsClient.gauge("RetrySink.queue.push.messages", records.size());
        statsClient.timeIt("RetrySink.queue.push.time", start);
        return pushStatus;
    }

    @Override
    public void close() {
        sink.close();
    }
}
