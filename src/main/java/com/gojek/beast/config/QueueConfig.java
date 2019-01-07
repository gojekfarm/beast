package com.gojek.beast.config;

import lombok.Data;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@Data
public class QueueConfig {
    private final long timeout;
    private final TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
    @Getter
    private int maxPushAttempts;

    public QueueConfig(long timeout) {
        this.timeout = timeout;
    }

    public QueueConfig(long timeout, int maxPushAttempts) {
        this.timeout = timeout;
        this.maxPushAttempts = maxPushAttempts;
    }
}
