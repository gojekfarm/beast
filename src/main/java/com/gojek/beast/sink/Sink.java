package com.gojek.beast.sink;

import com.gojek.beast.models.Status;

public interface Sink<T> {
    Status push(Iterable<T> messages);

    void close();
}
