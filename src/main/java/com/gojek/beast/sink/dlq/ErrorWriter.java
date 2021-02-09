package com.gojek.beast.sink.dlq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;

import java.util.List;
import java.util.Map;

/**
 * Interface to write erroneous records.
 */
public interface ErrorWriter {

    /**
     *  Writes all the records passed in to the underlying destination.
     *
     * @param errorRecords - map of {@link RecordsErrorType} and records that need to be wired to the destination.
     * @return status {@link Status} of the write. {@link Status#isSuccess()} should be false if the write failed
     * and compose of the exception leading to the failure.
     */
    Status writeRecords(Map<RecordsErrorType, List<Record>> errorRecords);
}
