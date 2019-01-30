package com.gojek.beast.models;

import java.util.Optional;

public class SuccessStatus implements Status {
    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public Optional<Exception> getException() {
        return Optional.empty();
    }

    public String toString() {
        return getClass().getName() + " returned success status";
    }
}
