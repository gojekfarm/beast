package com.gojek.beast.models;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RecordsTest {
    @Test
    public void shouldGetMaxOffsetFromRecords() {
        List<Record> records = Arrays.asList(new Record(new OffsetInfo("default-topic", 0, 0), null));
        new Records(records);
    }
}
