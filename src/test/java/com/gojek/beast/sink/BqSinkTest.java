package com.gojek.beast.sink;

import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.sink.bq.Record;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqSinkTest {

    @Mock
    BigQuery bigquery;
    Sink<Record> sink;
    private TableId tableId;
    InsertAllRequest.Builder builder;
    @Mock
    InsertAllResponse successfulResponse, failureResponse;

    @Before
    public void setUp() {
        tableId = TableId.of("test-dataset" ,"test-table");
        builder = InsertAllRequest.newBuilder(tableId);
        sink = new BqSink(bigquery, tableId);
        when(successfulResponse.hasErrors()).thenReturn(false);
        when(bigquery.insertAll(any())).thenReturn(successfulResponse);
        when(failureResponse.hasErrors()).thenReturn(true);
    }

    @Test
    public void ShouldPushMessageToBigQuerySuccessfully() {
        Map<String, Object> user1 = createUser("alice");
        InsertAllRequest request = builder.addRow(user1).build();
        Iterable<Record> records = Arrays.asList(new Record(user1));

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertTrue(status.isSuccess());
    }

    @Test
    public void ShouldPushMultipleMessagesToBigQuerySuccessfully() {
        Map<String, Object> user1 = createUser("alice");
        Map<String, Object> user2 = createUser("bob");
        Map<String, Object> user3 = createUser("mary");
        InsertAllRequest request = builder.addRow(user1).addRow(user2).addRow(user3).build();
        Iterable<Record> records = Arrays.asList(new Record(user1), new Record(user2), new Record(user3));

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertTrue(status.isSuccess());
    }


    @Test
    public void ShouldErrorWhenBigQueryInsertFails() {
        Map<String, Object> user1 = createUser("alice");
        InsertAllRequest request = builder.addRow(user1).build();
        Iterable<Record> records = Arrays.asList(new Record(user1));
        when(bigquery.insertAll(request)).thenReturn(failureResponse);

        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertFalse("Should return false", status.isSuccess() );
    }

    private Map<String, Object> createUser(String alice) {
        HashMap<String , Object> user = new HashMap<>();
        user.put("name", alice);
        user.put("age", 24);
        return user;
    }
}