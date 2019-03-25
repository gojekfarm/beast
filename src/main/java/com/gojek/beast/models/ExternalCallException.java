package com.gojek.beast.models;

public class ExternalCallException extends Exception {
    public ExternalCallException(String message) {
        super(message);
    }
}
