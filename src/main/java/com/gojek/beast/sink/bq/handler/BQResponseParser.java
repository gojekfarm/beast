package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.bq.handler.error.ErrorTypeFactory;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;

import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;

public final class BQResponseParser {
    /**
     * Parses the {@link InsertAllResponse} object and constructs a mapping of each record in @{@link Records} that were
     * tried to sink in BQ and the error type {@link BQInsertionRecordsErrorType}.
     *
     * @param records - records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @return - map of each record and the associated list of error types.
     */
    public Map<Record, List<BQInsertionRecordsErrorType>> parseBQResponse(final Records records, final InsertAllResponse bqResponse) {
        if (!bqResponse.hasErrors()) {
            return Collections.emptyMap();
        }
        Map<Record, List<BQInsertionRecordsErrorType>> parsedRecords = new HashMap<>();
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (long recordIndex : insertErrorsMap.keySet()) {
            final Record message = records.getRecords().get((int) recordIndex);
            final List<BQInsertionRecordsErrorType> errorTypeList = new ArrayList<>();
            parsedRecords.put(message, errorTypeList);
            for (final BigQueryError err : insertErrorsMap.get(recordIndex)) {
                errorTypeList.add(ErrorTypeFactory.getErrorType(err.getReason(), err.getMessage()));
            } //end of for each row
        }
        return parsedRecords;
    }
}
