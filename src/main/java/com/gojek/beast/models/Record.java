package com.gojek.beast.models;

import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Getter
public class Record {
    @Setter
    private OffsetInfo offsetInfo;
    private Map<String, Object> columns;
    private long size;

    public Record(OffsetInfo offsetInfo, Map<String, Object> columns) {
        this.offsetInfo = offsetInfo;
        this.columns = columns;
        this.size = columns.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    public String getId() {
        return String.format("%s_%d_%d", offsetInfo.getTopic(), offsetInfo.getPartition(), offsetInfo.getOffset());
    }

    public long getSize() {
        return size;
    }
}
