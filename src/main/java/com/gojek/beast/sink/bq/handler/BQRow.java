package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import com.google.cloud.bigquery.InsertAllRequest;

/**
 * Fetches BQ insertable row from the base record {@link Record}. The implementations can differ if unique rows need to be inserted or not.
 */
public interface BQRow {

    InsertAllRequest.RowToInsert of(Record record);
}
