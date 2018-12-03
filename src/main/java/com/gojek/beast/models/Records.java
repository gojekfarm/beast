package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Delegate;

import java.util.List;

@AllArgsConstructor
@Getter
public class Records implements Iterable<Record> {
    @Delegate
    private final List<Record> records;
}
