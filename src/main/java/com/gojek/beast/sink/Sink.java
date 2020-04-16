package com.gojek.beast.sink;

import com.gojek.beast.models.Status;

public interface Sink<T extends SinkElement> {
    Status push(T records);

    void close(String reason);
}
