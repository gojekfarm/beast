package com.gojek.beast.sink.dlq;

import com.gojek.beast.models.Status;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class WriteStatus implements Status {

    private boolean isSuccess;
    private Optional<Exception> exception;

    @Override
    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public Optional<Exception> getException() {
        return exception;
    }

    @Override
    public String toString() {
        String errString = "WriteStatus:";
        if (getException().isPresent()) {
            errString += "Exception: " + exception.get();
        }
        errString += " IsSuccessful: " + isSuccess;
        return errString;
    }
}
