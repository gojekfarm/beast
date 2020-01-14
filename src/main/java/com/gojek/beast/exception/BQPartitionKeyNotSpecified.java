package com.gojek.beast.exception;

public class BQPartitionKeyNotSpecified extends RuntimeException {
    public BQPartitionKeyNotSpecified(String message) {
        super(message);
    }
}
