package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
public class Record {
    private OffsetInfo offsetInfo;
    private Map<String, Object> columns;
}
