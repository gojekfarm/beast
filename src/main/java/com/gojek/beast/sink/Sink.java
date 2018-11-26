package com.gojek.beast.sink;

public interface Sink<T> {
    Status push(Iterable<T> messages);

    void close();
}
