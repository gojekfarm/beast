package com.gojek.beast.sink;

import com.gojek.beast.sink.bq.BQRowWithInsertId;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.gojek.beast.sink.bq.handler.DefaultLogWriter;
import com.gojek.beast.sink.bq.handler.BQErrorHandler;
import com.gojek.beast.sink.bq.handler.BQResponseParser;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.BqInsertErrors;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.sink.bq.handler.impl.OOBErrorHandler;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqSinkTest {

    private final OffsetInfo offsetInfo = new OffsetInfo("default-topic", 0, 0, Instant.now().toEpochMilli());
    @Mock
    private BigQuery bigquery;
    private Sink sink;
    private TableId tableId;
    private InsertAllRequest.Builder builder;
    @Mock
    private InsertAllResponse successfulResponse, failureResponse;
    private Map<Long, List<BigQueryError>> insertErrors;
    private BQRow bqRow;

    @Before
    public void setUp() {
        tableId = TableId.of("test-dataset", "test-table");
        builder = InsertAllRequest.newBuilder(tableId);
        bqRow = new BQRowWithInsertId();
        BQErrorHandler errorHandlerInstance = new OOBErrorHandler(new DefaultLogWriter());
        sink = new BqSink(bigquery, tableId, new BQResponseParser(), errorHandlerInstance, bqRow);
        when(successfulResponse.hasErrors()).thenReturn(false);
        when(bigquery.insertAll(any())).thenReturn(successfulResponse);
        when(failureResponse.hasErrors()).thenReturn(true);
        insertErrors = new HashMap<>();
        List<BigQueryError> columnError = Arrays.asList(new BigQueryError("failed since type mismatched", "column location", "message"));
        insertErrors.put(0L, columnError);
        when(failureResponse.getInsertErrors()).thenReturn(insertErrors);
    }

    @Test
    public void shouldPushMessageToBigQuerySuccessfully() {
        Record user = new Record(offsetInfo, createUser("alice"));
        InsertAllRequest request = builder.addRow(user.getId(), user.getColumns()).build();
        Records records = new Records(Arrays.asList(user));

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertTrue(status.isSuccess());
    }

    @Test
    public void shouldPushMultipleMessagesToBigQuerySuccessfully() {
        Record user1 = new Record(offsetInfo, createUser("alice"));
        Record user2 = new Record(offsetInfo, createUser("bob"));
        Record user3 = new Record(offsetInfo, createUser("mary"));
        Records records = new Records(Arrays.asList(user1, user2, user3));
        InsertAllRequest request = builder
                .addRow(user1.getId(), user1.getColumns())
                .addRow(user2.getId(), user2.getColumns())
                .addRow(user3.getId(), user3.getColumns())
                .build();

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertTrue(status.isSuccess());
    }

    @Test
    public void shouldErrorWhenBigQueryInsertFails() {
        Record user1 = new Record(offsetInfo, createUser("alice"));
        InsertAllRequest request = builder.addRow(user1.getId(), user1.getColumns()).build();
        Records records = new Records(Arrays.asList(user1));
        when(bigquery.insertAll(request)).thenReturn(failureResponse);

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertFalse("Should return false", status.isSuccess());
        assertTrue(status.getException().isPresent());
        assertEquals(insertErrors, ((BqInsertErrors) status.getException().get()).getErrors());
    }

    private Map<String, Object> createUser(String name) {
        HashMap<String, Object> user = new HashMap<>();
        user.put("name", name);
        user.put("age", 24);
        return user;
    }
}
