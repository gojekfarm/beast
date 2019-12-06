package com.gojek.beast.sink.bq.handler.impl;

/**
 * Class models all exceptions that are generated from the handler while processing BQ errors.
 */
public class BQErrorHandlerException extends RuntimeException {

    public BQErrorHandlerException(final String message) {
        super(message);
    }
}
