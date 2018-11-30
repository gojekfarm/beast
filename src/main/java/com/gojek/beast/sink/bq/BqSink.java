package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.Sink;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.StreamSupport;

@AllArgsConstructor
public class BqSink implements Sink<Record> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BqSink.class.getName());

    private final BigQuery bigquery;
    private final TableId tableId;

    @Override
    public InsertStatus push(Iterable<Record> records) {
        long recordCount = StreamSupport.stream(records.spliterator(), true).count();
        LOGGER.info("Pushing {} records to BQ", recordCount);
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        records.forEach(m -> builder.addRow(m.getColumns()));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigquery.insertAll(rows);
        return new InsertStatus(!response.hasErrors(), response.getInsertErrors());
    }

    @Override
    public void close() {
    }
}
