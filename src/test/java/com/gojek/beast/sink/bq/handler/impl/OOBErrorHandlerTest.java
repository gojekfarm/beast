package com.gojek.beast.sink.bq.handler.impl;

import com.gojek.beast.Clock;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.Record;
import com.gojek.beast.sink.bq.BaseBQTest;
import com.gojek.beast.sink.bq.handler.BQInsertionRecordsErrorType;
import com.gojek.beast.sink.bq.handler.WriteStatus;
import com.gojek.beast.sink.bq.handler.gcs.GCSErrorWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OOBErrorHandlerTest extends BaseBQTest {

    @Mock
    private Clock clock;
    @Mock
    private GCSErrorWriter errorWriter;
    /*@Mock
    private Storage gcsStoreMock;*/
    private long nowMillis;

    @Before
    public void setUp() {
        nowMillis = Instant.now().toEpochMilli();
        when(clock.currentEpochMillis()).thenReturn(nowMillis);
        when(errorWriter.writeErrorRecords(any())).thenReturn(new WriteStatus(true, Optional.ofNullable(null)));
    }

    @Test
    public void testHandlerReturnsSuccessForOOBErrors() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("oob-handler", Instant.now());
        TestMessage tMsgInvalid = getTestMessage("oob-handler", Instant.now().plus(Duration.ofDays(300)));
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg, tMsgInvalid);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();
        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        final List<BQInsertionRecordsErrorType> oobListErrors = new ArrayList<>();
        validListErrors.add(BQInsertionRecordsErrorType.VALID);
        oobListErrors.add(BQInsertionRecordsErrorType.OOB);
        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), oobListErrors);

        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertTrue(errorHandler.handleErrorRecords(errorMap).isSuccess());
    }

    @Test
    public void testHandlerFailsForOOBAndInvalidErrors() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("oob-handler", Instant.now());
        TestMessage tMsgInvalid = getTestMessage("oob-handler", Instant.now().plus(Duration.ofDays(300)));
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg, tMsgInvalid);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();
        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        final List<BQInsertionRecordsErrorType> oobListErrors = new ArrayList<>();
        validListErrors.add(BQInsertionRecordsErrorType.INVALID);
        oobListErrors.add(BQInsertionRecordsErrorType.OOB);
        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), oobListErrors);
        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertFalse(errorHandler.handleErrorRecords(errorMap).isSuccess());
    }

    @Test
    public void testHandlerFailsForValidAndInvalidErrors() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("oob-handler", Instant.now());
        TestMessage tMsgInvalid = getTestMessage("oob-handler", Instant.now().plus(Duration.ofDays(300)));
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg, tMsgInvalid);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();
        final List<BQInsertionRecordsErrorType> inValidListErrors = new ArrayList<>();
        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        inValidListErrors.add(BQInsertionRecordsErrorType.INVALID);
        validListErrors.add(BQInsertionRecordsErrorType.VALID);
        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), inValidListErrors);
        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertFalse(errorHandler.handleErrorRecords(errorMap).isSuccess());
    }

    @Test
    public void testHandlerSucceedsForOOBAndValidErrors() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("oob-handler", Instant.now());
        TestMessage tMsgInvalid = getTestMessage("oob-handler", Instant.now().plus(Duration.ofDays(300)));
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg, tMsgInvalid);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();
        final List<BQInsertionRecordsErrorType> inValidListErrors = new ArrayList<>();
        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        inValidListErrors.add(BQInsertionRecordsErrorType.OOB);
        validListErrors.add(BQInsertionRecordsErrorType.VALID);
        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), inValidListErrors);
        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertTrue(errorHandler.handleErrorRecords(errorMap).isSuccess());
    }

    @Test
    public void testValidRecordsAreReturned() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("oob-handler-valid", Instant.now());
        TestMessage tMsgInvalid = getTestMessage("oob-handler-invalid", Instant.now().plus(Duration.ofDays(300)));
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg, tMsgInvalid);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();
        final List<BQInsertionRecordsErrorType> inValidListErrors = new ArrayList<>();
        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        inValidListErrors.add(BQInsertionRecordsErrorType.OOB);
        validListErrors.add(BQInsertionRecordsErrorType.VALID);
        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), inValidListErrors);
        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertEquals(1, errorHandler.getBQValidRecords(errorMap).getRecords().size());
        assertEquals("Order-No-oob-handler-valid", errorHandler
                .getBQValidRecords(errorMap)
                .getRecords()
                .get(0)
                .getColumns()
                .get("order_number"));
    }

    @Test
    public void testValidRecordsAreReturnedWhenAllErrorsArePresent() throws InvalidProtocolBufferException {
        TestMessage tMsg1 = getTestMessage("oob-handler-valid1", Instant.now());
        TestMessage tMsg2 = getTestMessage("oob-handler-valid2", Instant.now());
        TestMessage tMsgInvalid1 = getTestMessage("oob-handler-invalid1", Instant.now().plus(Duration.ofDays(300)));
        TestMessage tMsgInvalid2 = getTestMessage("oob-handler-invalid2", Instant.now());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg1, tMsg2, tMsgInvalid1, tMsgInvalid2);
        final Map<Record, List<BQInsertionRecordsErrorType>> errorMap = new HashMap<>();

        final List<BQInsertionRecordsErrorType> validListErrors = new ArrayList<>();
        validListErrors.add(BQInsertionRecordsErrorType.VALID);

        final List<BQInsertionRecordsErrorType> inValidListErrors = new ArrayList<>();
        inValidListErrors.add(BQInsertionRecordsErrorType.INVALID);

        final List<BQInsertionRecordsErrorType> oobListErrors = new ArrayList<>();
        oobListErrors.add(BQInsertionRecordsErrorType.OOB);

        errorMap.put(records.get(0), validListErrors);
        errorMap.put(records.get(1), validListErrors);
        errorMap.put(records.get(2), oobListErrors);
        errorMap.put(records.get(3), inValidListErrors);
        OOBErrorHandler errorHandler = new OOBErrorHandler(errorWriter);
        assertEquals(2, errorHandler.getBQValidRecords(errorMap).getRecords().size());
        String value1 = (String) errorHandler.getBQValidRecords(errorMap).getRecords().get(0).getColumns().get("order_number");
        assertTrue(value1.equals("Order-No-oob-handler-valid2") || value1.equals("Order-No-oob-handler-valid1"));
    }
}
