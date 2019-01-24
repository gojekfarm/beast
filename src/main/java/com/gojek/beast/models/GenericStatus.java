package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

@AllArgsConstructor
public class GenericStatus implements Status {
    @Getter
    private boolean success;
    private Exception exception;

    public GenericStatus(boolean success) {
        this.success = success;
    }

    @Override
    public Optional<Exception> getException() {
        return Optional.of(exception);
    }
}
