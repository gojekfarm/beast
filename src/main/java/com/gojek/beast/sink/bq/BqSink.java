package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.Sink;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class BqSink implements Sink {

    private final BigQuery bigquery;
    private final TableId tableId;

    @Override
    public InsertStatus push(Records records) {
        log.info("Pushing {} records to BQ", records.size());
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        records.forEach((Record m) -> builder.addRow(m.getColumns()));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);
        return new InsertStatus(!response.hasErrors(), response.getInsertErrors());
    }

    @Override
    public void close() {
    }
}
