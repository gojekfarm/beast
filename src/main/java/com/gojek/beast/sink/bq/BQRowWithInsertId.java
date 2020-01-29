package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Record;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.google.cloud.bigquery.InsertAllRequest;

public class BQRowWithInsertId implements BQRow {

    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getId(), record.getColumns());
    }
}
