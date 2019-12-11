package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;

/**
 * Descriptor interface that defines the various error descriptors and the corresponding error types.
 */
public interface ErrorDescriptor {

    /**
     * Gets the error type.
     *
     * @return BQInsertionRecordsErrorType - error type
     */
    BQInsertionRecordsErrorType getType();

    /**
     * If the implementing descriptor matches the condition as prescribed in the concrete implementation.
     *
     * @return - true if the condition matches, false otherwise.
     */
    boolean matches();

}
