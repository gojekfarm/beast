package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

@Slf4j
@AllArgsConstructor
public class BqSink implements Sink {

    private final BigQuery bigquery;
    private final TableId tableId;

    private final Stats statsClient = Stats.client();

    @Override
    public InsertStatus push(Records records) {
        Instant start = Instant.now();
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        records.forEach((Record m) -> builder.addRow(m.getColumns()));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);
        log.info("Pushing {} records to BQ success?: {}", records.size(), !response.hasErrors());
        statsClient.gauge("sink.bq.push.records", records.size());
        statsClient.timeIt("sink.bq.push.time", start);
        return new InsertStatus(!response.hasErrors(), response.getInsertErrors());
    }

    @Override
    public void close() {
    }
}
