package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class OOBDescriptor implements ErrorDescriptor {

    private final String reason;
    private final String message;

    @Override
    public BQInsertionRecordsErrorType getType() {
        return BQInsertionRecordsErrorType.OOB;
    }

    @Override
    public boolean matches() {
        return reason.equals("invalid")
                && message.contains("is outside the allowed bounds")
                && message.contains("365 days in the past and 183 days in the future");
    }

}
