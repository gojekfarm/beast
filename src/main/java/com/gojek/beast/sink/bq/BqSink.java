package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.handler.BQFilteredResponse;
import com.gojek.beast.sink.bq.handler.BQResponseParser;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.gojek.beast.sink.dlq.ErrorWriter;
import com.gojek.beast.sink.dlq.RecordsErrorType;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static com.google.cloud.bigquery.InsertAllRequest.Builder;
import static com.google.cloud.bigquery.InsertAllRequest.newBuilder;

@Slf4j
@AllArgsConstructor
public class BqSink implements Sink {

    private final BigQuery bigquery;
    private final TableId tableId;
    private final BQResponseParser responseParser;
    private final BQRow recordInserter;
    private final ErrorWriter errorWriter;

    private final Stats statsClient = Stats.client();

    @Override
    public Status push(Records records) {
        InsertAllResponse response = insertIntoBQ(records.getRecords());
        if (response.hasErrors()) {
            //parse the error records
            BQFilteredResponse filteredResponse = responseParser.parseResponse(records.getRecords(), response);

            // if batch contains records that we can't really handle, fail whole batch
            List<Record> unhandledRecords = filteredResponse.getUnhandledRecords();
            if (!unhandledRecords.isEmpty()) {
                log.info("Batch with records size: {} contains invalid records, marking this batch to fail", unhandledRecords.size());
                statsClient.gauge("data.error.records,type=invalid", unhandledRecords.size());
                return new InsertStatus(false, response.getInsertErrors());
            }

            // retryable records
            List<Record> retryableRecords = filteredResponse.getRetryableRecords();
            if (!retryableRecords.isEmpty()) {
                // try inserting valid records into bq
                InsertAllResponse retriedResponse = insertIntoBQ(retryableRecords);
                if (retriedResponse.hasErrors()) {
                    statsClient.gauge("record.processing.failure,type=retry," + statsClient.getBqTags(), retryableRecords.size());
                    return new InsertStatus(false, retriedResponse.getInsertErrors());
                }
            }

            // DLQ sinkable records
            List<Record> oobRecords = filteredResponse.getOobRecords();
            if (!oobRecords.isEmpty()) {
                log.info("Error handler parsed OOB records size {}, handoff to the writer {}", oobRecords.size(), errorWriter.getClass().getSimpleName());
                statsClient.gauge("data.error.records,type=oob", oobRecords.size());
                final Status dlqStatus = errorWriter.writeRecords(ImmutableMap.of(RecordsErrorType.OOB, oobRecords));
                if (!dlqStatus.isSuccess()) {
                    log.info("Batch with records size: {} contains DLQ sinkable records but failed to sink", oobRecords.size());
                    return dlqStatus;
                }
            }
        }

        return new InsertStatus(true, Collections.emptyMap());
    }

    private InsertAllResponse insertIntoBQ(List<Record> records) {
        Instant start = Instant.now();
        Builder builder = newBuilder(tableId);
        records.forEach((Record m) -> builder.addRow(recordInserter.of(m)));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);

        log.info("Pushed a batch of {} records to BQ. Insert success?: {}", records.size(), !response.hasErrors());
        statsClient.count("bq.sink.push.records", records.size());
        statsClient.timeIt("bq.sink.push.time", start);
        return response;
    }

    @Override
    public void close(String reason) {
        log.info("BQSink closed: {}", reason);
    }
}
