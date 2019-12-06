package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;

import java.util.List;

/**
 * Interface to write erroneous records.
 */
public interface ErrorWriter {

    /**
     *  Writes all the records passed in to the underlying destination.
     *
     * @param records - list of records that need to be wired to the destination.
     * @return status {@link Status} of the write. {@link Status#isSuccess()} should be false if the write failed
     * and compose of the exception leading to the failure.
     */
    Status writeErrorRecords(List<Record> records);
}
