package com.gojek.beast.sink.executor;

import com.gojek.beast.models.Status;

public interface Executor {
    Executor ifFailure();
    Executor execute();
    Status status();
}
