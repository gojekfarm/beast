package com.gojek.beast.models;

import java.util.Optional;

public interface Status {
    boolean isSuccess();

    Optional<Exception> getException();
}
