package com.gojek.beast.sink.bq.handler.error;

import com.gojek.beast.sink.bq.handler.BQRecordsErrorType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ErrorTypeFactoryTest {

    @Test
    public void testErrorTypeIsUnknown() {
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("", ""));
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("unknown", ""));
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("", "valid-invalid"));
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("valid", ""));
    }

    @Test
    public void testErrorTypeIsValid() {
        assertEquals(BQRecordsErrorType.VALID, ErrorTypeFactory.getErrorType("stopped", ""));
        assertEquals(BQRecordsErrorType.VALID, ErrorTypeFactory.getErrorType("stopped", "Any Message"));
    }

    @Test
    public void testErrorTypeIsInvalid() {
        assertEquals(BQRecordsErrorType.INVALID, ErrorTypeFactory.getErrorType("invalid", "no such field"));
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("invalid", "any message"));
        assertEquals(BQRecordsErrorType.UNKNOWN, ErrorTypeFactory.getErrorType("invalid", "out of bounds"));
    }

    @Test
    public void testErrorTypeIsOutOfBounds() {
        assertEquals(BQRecordsErrorType.OOB, ErrorTypeFactory.getErrorType("invalid", "is outside the allowed bounds, 1825 days in the past and 366 days in the future"));
        assertEquals(BQRecordsErrorType.OOB, ErrorTypeFactory.getErrorType("invalid", "out of range"));
    }
}
