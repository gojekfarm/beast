package com.gojek.beast.exception;

public class BQTableUpdateFailure extends RuntimeException {
    public BQTableUpdateFailure(String message) {
        super(message);
    }
}
