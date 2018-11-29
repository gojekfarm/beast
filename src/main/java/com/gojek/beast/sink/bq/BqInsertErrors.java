package com.gojek.beast.sink.bq;

import com.google.cloud.bigquery.BigQueryError;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class BqInsertErrors extends RuntimeException {
    private final Map<Long, List<BigQueryError>> errors;

    public Map<Long, List<BigQueryError>> getErrors() {
        return errors;
    }
}
