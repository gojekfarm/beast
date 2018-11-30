package com.gojek.beast.config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.concurrent.TimeUnit;

@AllArgsConstructor
@Data
public class WorkerConfig {
    private final long timeout;
    private final TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
}
