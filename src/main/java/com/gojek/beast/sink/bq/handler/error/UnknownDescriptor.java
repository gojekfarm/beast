package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UnknownDescriptor implements ErrorDescriptor {

    @Override
    public BQInsertionRecordsErrorType getType() {
        return BQInsertionRecordsErrorType.UNKNOWN;
    }

    @Override
    public boolean matches() {
        return false;
    }
}
