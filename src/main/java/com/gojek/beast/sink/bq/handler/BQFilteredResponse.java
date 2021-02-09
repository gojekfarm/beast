package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.models.Record;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class BQFilteredResponse {
    private List<Record> retryableRecords;
    private List<Record> unhandledRecords;
    private List<Record> oobRecords;
}
