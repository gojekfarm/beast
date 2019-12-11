package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class InvalidDescriptor implements ErrorDescriptor {

    private final String reason;
    private final String message;

    @Override
    public BQInsertionRecordsErrorType getType() {
        return BQInsertionRecordsErrorType.INVALID;
    }

    @Override
    public boolean matches() {
        return reason.equals("invalid") && message.contains("no such field");
    }
}
