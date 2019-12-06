package com.gojek.beast.sink.bq.handler;

/**
 * Models the Big Query records insertion status.
 */
public enum BQInsertionRecordsErrorType {

    /**
     * BQ failures with valid data.
     */
    VALID,

    /**
     * BQ failures due to Out of Bounds errors on partition keys.
     */
    OOB,

    /**
     * BQ errors due to invalid schema/data.
     */
    INVALID,

    /**
     * Unknown error type.
     */
    UNKNOWN,

}
