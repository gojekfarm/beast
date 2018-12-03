package com.gojek.beast.models;

import lombok.Getter;
import lombok.experimental.Delegate;

import java.util.List;


public class Records implements Iterable<Record> {
    @Delegate
    @Getter
    private final List<Record> records;
    private OffsetInfo offsetInfo;

    public Records(List<Record> records) {
        this.records = records;
    }

    public OffsetInfo getMaxOffsetInfo() {
        return offsetInfo;
    }
}
