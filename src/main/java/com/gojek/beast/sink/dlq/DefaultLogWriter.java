package com.gojek.beast.sink.dlq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Default implementation that logs the error records to the log stream. The returned status however is failed as it implicitly
 * fails the overall execution of this beast instance. Essentially this implementation mimics the default behaviour of
 * erroneous insertions that are rejected by BQ and therefore mandates the failure status.
 */
@Slf4j
public class DefaultLogWriter implements ErrorWriter {

    @Override
    public Status writeRecords(Map<RecordsErrorType, List<Record>> errorRecords) {
        if (errorRecords != null && !errorRecords.isEmpty()) {
            errorRecords.forEach((bqRecordsErrorType, records) -> {
                records.forEach(record -> {
                    log.debug("Error record: {} columns: {} type: {}",  record.getId(), record.getColumns(), bqRecordsErrorType.toString());
                });
            });
        }
        return new WriteStatus(false, Optional.ofNullable(null));
    }
}
