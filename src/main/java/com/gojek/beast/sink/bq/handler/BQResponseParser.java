package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.gojek.beast.sink.bq.handler.error.ErrorTypeFactory;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BQResponseParser {

    private Map<Record, Set<BQRecordsErrorType>> parseErrors(final List<Record> records, final InsertAllResponse bqResponse) {
        if (!bqResponse.hasErrors()) {
            return Collections.emptyMap();
        }
        Map<Record, Set<BQRecordsErrorType>> parsedRecords = new HashMap<>();
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (final Map.Entry<Long, List<BigQueryError>> errorEntry : insertErrorsMap.entrySet()) {
            final Record message = records.get(errorEntry.getKey().intValue());
            parsedRecords.put(message, errorTypeList(errorEntry.getValue()));
        }
        return parsedRecords;
    }

    private Set<BQRecordsErrorType> errorTypeList(List<BigQueryError> bqErrors) {
        return bqErrors.stream().map(err ->
                ErrorTypeFactory.getErrorType(err.getReason(), err.getMessage())).
                collect(Collectors.toSet());
    }

    /**
     * Parses the {@link InsertAllResponse} object and constructs a mapping of each record in {@link com.gojek.beast.models.Records} that were
     * tried to sink in BQ and the error type {@link BQRecordsErrorType}.
     * {@link InsertAllResponse} in bqResponse are 1 to 1 indexed based on the records that are requested to be inserted.
     *
     * @param records - list of records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @return {@link BQFilteredResponse} - groups of records that should be handled together
     */
    public BQFilteredResponse parseResponse(final List<Record> records, final InsertAllResponse bqResponse) {
        Map<Record, Set<BQRecordsErrorType>> filtered = parseErrors(records, bqResponse);
        final List<Record> retryableRecords = new ArrayList<>();
        final List<Record> unhandledRecords = new ArrayList<>();
        final List<Record> oobRecords = new ArrayList<>();
        for (final Map.Entry<Record, Set<BQRecordsErrorType>> recordSet : filtered.entrySet()) {
            final Set<BQRecordsErrorType> errorTypes = recordSet.getValue();
            if (errorTypes.contains(BQRecordsErrorType.INVALID) || errorTypes.contains(BQRecordsErrorType.UNKNOWN)) {
                unhandledRecords.add(recordSet.getKey());
                break;
            }
            if (errorTypes.contains(BQRecordsErrorType.OOB)) {
                oobRecords.add(recordSet.getKey());
                break;
            }
            if (errorTypes.contains(BQRecordsErrorType.VALID)) {
                retryableRecords.add(recordSet.getKey());
                break;
            }
        }
        return new BQFilteredResponse(retryableRecords, unhandledRecords, oobRecords);
    }
}
