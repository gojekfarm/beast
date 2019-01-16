package com.gojek.beast.sink.executor;

import com.gojek.beast.backoff.BackOffProvider;
import com.gojek.beast.models.Status;

public interface Executor {
    Executor ifFailure(BackOffProvider backOffProvider, int attemptCount);
    Executor execute();
    Status status();
}
