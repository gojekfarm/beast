package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQRecordsErrorType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * UnknownError is used when error factory failed to match any possible
 * known errors
 * */
public class UnknownError implements ErrorDescriptor {

    @Override
    public BQRecordsErrorType getType() {
        return BQRecordsErrorType.UNKNOWN;
    }

    @Override
    public boolean matches() {
        return false;
    }
}
