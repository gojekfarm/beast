package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.gojek.beast.sink.bq.handler.BQResponseParser;
import com.gojek.beast.sink.bq.handler.BQErrorHandler;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.google.cloud.bigquery.InsertAllRequest.*;

@Slf4j
@AllArgsConstructor
public class BqSink implements Sink {

    private final BigQuery bigquery;
    private final TableId tableId;
    private final BQResponseParser responseParser;
    private final BQErrorHandler errorHandler; // handler instance for BQ related errors
    private final BQRow recordInserter;

    private final Stats statsClient = Stats.client();

    @Override
    public Status push(Records records) {
        InsertAllResponse response = insertIntoBQ(records);
        InsertStatus status = new InsertStatus(!response.hasErrors(), response.getInsertErrors());
        //if bq has errors
        if (response.hasErrors()) {
            //parse the error records
            Map<Record, List<BQInsertionRecordsErrorType>> parsedRecords = responseParser.parseBQResponse(records, response);
            //sink error records
            boolean isTheSinkSuccessful = errorHandler.handleErrorRecords(parsedRecords).isSuccess();
            if (!isTheSinkSuccessful) {
                return new InsertStatus(isTheSinkSuccessful, response.getInsertErrors());
            }
            Records bqValidRecords = errorHandler.getBQValidRecords(parsedRecords);
            if (!bqValidRecords.getRecords().isEmpty()) { // there are valid records
                //insert the valid records into bq
                isTheSinkSuccessful &= !insertIntoBQ(bqValidRecords).hasErrors();
            }
            return new InsertStatus(isTheSinkSuccessful, response.getInsertErrors());
        }
        // return the original response
        return new InsertStatus(!response.hasErrors(), response.getInsertErrors());
    }

    private InsertAllResponse insertIntoBQ(Records records) {
        Instant start = Instant.now();
        Builder builder = newBuilder(tableId);
        records.forEach((Record m) -> builder.addRow(recordInserter.of(m)));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);
        log.info("Pushing {} records to BQ success?: {}", records.size(), !response.hasErrors());
        statsClient.count("bq.sink.push.records", records.size());
        statsClient.timeIt("bq.sink.push.time", start);
        return response;
    }

    @Override
    public void close(String reason) {
    }
}
