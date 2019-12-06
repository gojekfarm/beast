package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;

import java.util.List;
import java.util.Map;

/**
 * Interface that declares the contracts to handle erroneous records that was rejected by Big Query insertion.
 */
public interface BQErrorHandler {

    /**
     * The method provisions handling the records that failed to insert into Big Query. The concrete implementation need to be
     * synchronously part of the {@link com.gojek.beast.sink.bq.BqSink#push(Records)}. The handler can choose to modify the returned insert status {@link Status}
     * for those rows that failed initially with Big Query however succeeded with the handler.
     *
     * @param records - records map that has the {@link Record} and it's list of associated errors of type {@link BQInsertionRecordsErrorType}
     * @return - Inserted status post the handler
     */
    Status handleErrorRecords(Map<Record, List<BQInsertionRecordsErrorType>> records);

    /**
     * Gets all the records that could be processed by BQ from the input batch. The validity of the record is as determined by the parser {@link BQResponseParser}
     *
     * @param records - records map that has the {@link Record} and it's list of associated errors of type {@link BQInsertionRecordsErrorType}
     * @return - records that contain those that can be inserted by BQ.
     */
    Records getBQValidRecords(Map<Record, List<BQInsertionRecordsErrorType>> records);

}
