package com.gojek.beast.sink.bq;

import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.dlq.DefaultLogWriter;
import com.gojek.beast.sink.dlq.ErrorWriter;
import com.gojek.beast.sink.dlq.RecordsErrorType;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;

public class DefaultLogWriterTest {

    @Test
    public void testDefaultLogWriterReturnsStatus() {
        ErrorWriter defaultWriter = new DefaultLogWriter();
        Record record = new Record(new OffsetInfo("test", 1, 1L, System.currentTimeMillis()), new HashMap<>());
        Status result = defaultWriter.writeRecords(ImmutableMap.of(RecordsErrorType.OOB, Arrays.asList(record)));
        assertFalse("Should be failed", result.isSuccess());
    }

    @Test
    public void testDefaultLogWriterDoesNotFailOnEmptyRecords() {
        ErrorWriter defaultWriter = new DefaultLogWriter();
        Status result = defaultWriter.writeRecords(null);
        assertFalse(result.isSuccess());
        result = defaultWriter.writeRecords(ImmutableMap.of());
        assertFalse(result.isSuccess());
    }
}

