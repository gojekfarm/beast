package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class OffsetInfo {
    private final String topic;
    private final int partition;
    private final long offset;
}
