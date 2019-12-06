package com.gojek.beast.sink.bq.handler.gcs;

import com.gojek.beast.Clock;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.BaseBQTest;
import com.gojek.beast.sink.bq.handler.impl.BQErrorHandlerException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class GCSwriterTest extends BaseBQTest {

    @Captor
    private ArgumentCaptor<BlobInfo> createObjCaptor;
    @Mock
    private Clock clock;
    private GCSErrorWriter errorWriter;
    @Mock
    private Storage gcsStoreMock;
    @Mock
    private Blob blobMock;
    private long nowMillis;

    @Before
    public void setUp() {
        nowMillis = Instant.now().toEpochMilli();
        when(clock.currentEpochMillis()).thenReturn(nowMillis);
        errorWriter = new GCSErrorWriter(gcsStoreMock, "test-bucket", "test-integ-beast");
    }

    @Test
    public void testErrorWriterExecutesSuccessfully() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("mock-gcs-writer1", Instant.now());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        columnMapping.put("2", "order_url");
        columnMapping.put("3", "order_details");
        columnMapping.put("4", "created_at");
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                 clock, tMsg);
        Status status = errorWriter.writeErrorRecords(records);
        assertTrue(status.isSuccess());
    }

    @Test (expected = BQErrorHandlerException.class)
    public void testStoreThrowsBQHandlerExceptionAsExpected() throws InvalidProtocolBufferException {
        TestMessage tMsg = getTestMessage("mock-gcs-writer1", Instant.now());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        columnMapping.put("2", "order_url");
        columnMapping.put("3", "order_details");
        columnMapping.put("4", "created_at");
        when(gcsStoreMock.create(any(BlobInfo.class), any(byte[].class))).thenThrow(StorageException.class);
        List<Record> records = getKafkaConsumerRecords(columnMapping, Instant.now(), "test-mock", 1, 2,
                clock, tMsg);
        try {
            Status status = errorWriter.writeErrorRecords(records);
        } catch (BQErrorHandlerException bqee) {
            throw bqee;
        }
        fail("expected BQErrorhandlerexception");
    }

    @Test
    public void testGCSWriterCanHandleEmptyRecords() {
        assertTrue(errorWriter.writeErrorRecords(new ArrayList<>()).isSuccess());
    }
}
