package com.gojek.beast.exception;

public class ProtoNotFoundException extends RuntimeException {
    public ProtoNotFoundException(String message) {
        super(message);
    }
}
