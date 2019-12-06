package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;

import java.util.Arrays;
import java.util.List;

/**
 * Factory class that determines the type {@link BQInsertionRecordsErrorType} error based on the
 * error string supplied.
 */
public class ErrorTypeFactory {

    public static BQInsertionRecordsErrorType getErrorType(String reasonText, String msgText) {
        List<ErrorDescriptor> errDescList = Arrays.asList(
                new InvalidDescriptor(reasonText, msgText),
                new OOBDescriptor(reasonText, msgText),
                new ValidDescriptor(reasonText));

        ErrorDescriptor errorDescriptor = errDescList
                .stream()
                .filter(ErrorDescriptor::matches)
                .findFirst()
                .orElse(new UnknownDescriptor());
        return errorDescriptor.getType();
    }

}
