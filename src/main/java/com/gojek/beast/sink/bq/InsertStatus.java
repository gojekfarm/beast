package com.gojek.beast.sink.bq;

import com.gojek.beast.models.Status;
import com.google.cloud.bigquery.BigQueryError;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@AllArgsConstructor
public class InsertStatus implements Status {
    private final BqInsertErrors cause;
    private boolean success;

    public InsertStatus(boolean success, Map<Long, List<BigQueryError>> insertErrors) {
        this.success = success;
        this.cause = new BqInsertErrors(insertErrors);
    }

    @Override
    public Optional<Exception> getException() {
        return Optional.of(cause);
    }
}
