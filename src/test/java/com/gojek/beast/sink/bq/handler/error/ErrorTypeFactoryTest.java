package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ErrorTypeFactoryTest {

    @Test
    public void testErrorTypeIsUnknown() {
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("", ""));
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("unknown", ""));
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("", "valid-invalid"));
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("valid", ""));
    }

    @Test
    public void testErrorTypeIsValid() {
        assertEquals(BQInsertionRecordsErrorType.VALID, ErrorTypeFactory.getErrorType("stopped", ""));
        assertEquals(BQInsertionRecordsErrorType.VALID, ErrorTypeFactory.getErrorType("stopped", "Any Message"));
    }

    @Test
    public void testErrorTypeIsInvalid() {
        assertEquals(BQInsertionRecordsErrorType.INVALID, ErrorTypeFactory.getErrorType("invalid", "no such field"));
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("invalid", "any message"));
        assertEquals(BQInsertionRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("invalid", "out of bounds"));
    }

    @Test
    public void testErrorTypeIsOutOfBounds() {
        assertEquals(BQInsertionRecordsErrorType.OOB, ErrorTypeFactory.getErrorType("invalid", "is outside the allowed bounds, 1825 days in the past and 366 days in the future"));
        assertEquals(BQInsertionRecordsErrorType.OOB, ErrorTypeFactory.getErrorType("invalid", "out of range"));
    }
}
