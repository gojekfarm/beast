package com.gojek.beast.sink.dlq;

public enum RecordsErrorType {
    /**
     * Should only be used if we don't know what caused this error in record.
     */
    UNKNOWN,

    /**
     * Failed to deserialize the message from protobuf.
     */
    DESERIALIZE,

    /**
     * BQ failures due to Out of Bounds errors on partition keys.
     */
    OOB,
}
