package com.gojek.beast.sink.bq.handler;

import com.gojek.beast.sink.dlq.WriteStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class WriteStatusTest {

    @Test
    public void testStatusWithNoException() {
        WriteStatus status = new WriteStatus(true, Optional.ofNullable(null));
        Assert.assertTrue(status.isSuccess());
    }
}
