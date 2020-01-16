package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@AllArgsConstructor
@Getter
public class Record {
    @Setter
    private OffsetInfo offsetInfo;
    private Map<String, Object> columns;

    public String getId() {
        return String.format("%s_%d_%d", offsetInfo.getTopic(), offsetInfo.getPartition(), offsetInfo.getOffset());
    }

    public long getSize() {
        return columns.toString().getBytes(StandardCharsets.UTF_8).length;
    }
}
