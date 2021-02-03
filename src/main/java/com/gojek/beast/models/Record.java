package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Builder
@Getter
public class Record {
    @Setter
    private OffsetInfo offsetInfo;
    private Map<String, Object> columns;
    private byte[] key;
    private byte[] value;

    public Record(OffsetInfo offsetInfo, Map<String, Object> cols) {
        this(offsetInfo, cols, null, null);
    }

    public String getId() {
        return String.format("%s_%d_%d", offsetInfo.getTopic(), getPartition(), offsetInfo.getOffset());
    }

    public Integer getPartition() {
        return offsetInfo.getPartition();
    }

    public long getSize() {
        return (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
    }
}
