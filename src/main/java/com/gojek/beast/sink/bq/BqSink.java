package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.Sink;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class BqSink implements Sink {
    private static final Logger LOGGER = LoggerFactory.getLogger(BqSink.class.getName());

    private final BigQuery bigquery;
    private final TableId tableId;

    @Override
    public InsertStatus push(Records records) {
        LOGGER.info("Pushing {} records to BQ", records.size());
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
