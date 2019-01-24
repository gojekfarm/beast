package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Getter
public class Record {
    @Setter
    private OffsetInfo offsetInfo;
    private Map<String, Object> columns;

    public String getId() {
        return String.format("%d_%d", offsetInfo.getPartition(), offsetInfo.getOffset());
    }
}
