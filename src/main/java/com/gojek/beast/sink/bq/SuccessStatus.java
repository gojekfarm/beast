package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.Status;

public class SuccessStatus implements Status {
    @Override
    public boolean isSuccess() {
        return true;
    }
}
