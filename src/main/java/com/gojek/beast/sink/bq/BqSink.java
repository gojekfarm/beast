package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.Status;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BqSink implements Sink<Record> {
    private final BigQuery bigquery;
    private final TableId tableId;

    @Override
    public Status push(Iterable<Record> records) {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        records.forEach(m -> builder.addRow(m.getColumns()));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);
        return new InsertStatus(!response.hasErrors());
    }

    @Override
    public void close() {
    }
}
