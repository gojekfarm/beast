package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * Default implementation that logs the error records to the log stream. The returned status however is failed as it implicitly
 * fails the overall execution of this beast instance. Essentially this implementation mimics the default behaviour of
 * erroneous insertions that are rejected by BQ and therefore mandates the failure status.
 */
@Slf4j
public class DefaultLogWriter implements ErrorWriter {

    private final Stats statsClient = Stats.client(); // metrics client

    @Override
    public Status writeErrorRecords(List<Record> records) {
        if (records != null && !records.isEmpty()) {
            records.forEach(record -> {
                log.debug("Error record: {} columns: {}", record.getId(), record.getColumns());
            });
            statsClient.gauge("data.error.records,type=oob", records.size());
        }
        return new WriteStatus(false, Optional.ofNullable(null));
    }
}
