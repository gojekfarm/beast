package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.Status;

public class FailureStatus implements Status {
    @Override
    public boolean isSuccess() {
        return false;
    }
}
