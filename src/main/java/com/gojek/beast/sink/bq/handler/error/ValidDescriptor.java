package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ValidDescriptor implements ErrorDescriptor {

    private final String reason;

    @Override
    public BQInsertionRecordsErrorType getType() {
        return BQInsertionRecordsErrorType.VALID;
    }

    @Override
    public boolean matches() {
        return reason.equals("stopped");
    }
}
