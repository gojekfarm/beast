package com.gojek.beast.models;

import java.util.Optional;

public class FailureStatus implements Status {
    private Exception cause;
    private String message;

    public FailureStatus(Exception cause, String message) {
        this.cause = cause;
        this.message = message;
    }

    public FailureStatus(Exception cause) {
        this.cause = cause;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public Optional<Exception> getException() {
        return Optional.ofNullable(cause);
    }

    @Override
    public String toString() {
        return "FailureStatus{"
                + "exception=" + cause.getClass().getName()
                + ", cause=" + cause.getCause()
                + ", message='" + ((message != null) ? message : cause.getMessage()) + '\'' + '}';
    }
}
