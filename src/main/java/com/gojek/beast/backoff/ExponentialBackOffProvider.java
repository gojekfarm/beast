package com.gojek.beast.backoff;

import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Math.toIntExact;

@Slf4j
@AllArgsConstructor
public class ExponentialBackOffProvider implements BackOffProvider {

    private final int initialExpiryTimeInMs;
    private final int maximumExpiryTimeInMS;
    private final int backoffRate;
    private final BackOff backOff;

    private final Stats statsClient = Stats.client();

    @Override
    public void backOff(int attemptCount) {
        long sleepTimeInMillis = this.calculateDelay(attemptCount);
        log.info("Backing off for {} milliseconds, attempt count: {}", sleepTimeInMillis, attemptCount);
        backOff.inMilliSeconds(sleepTimeInMillis);
        statsClient.gauge("exponentialBackOff.backOffTime", toIntExact(sleepTimeInMillis));
        statsClient.gauge("exponentialBackOff.attemptCount", attemptCount);
    }

    private long calculateDelay(int attemptCount) {
        double exponentialBackOffTimeInMs = initialExpiryTimeInMs * Math.pow(backoffRate, attemptCount);
        return (long) Math.min(maximumExpiryTimeInMS, exponentialBackOffTimeInMs);
    }
}
