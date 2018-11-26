package com.gojek.beast.sink;

import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.sink.bq.Record;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BqSinkTest {

    @Mock
    BigQuery bigquery;
    Sink<Record> sink;
    private TableId tableId;
    InsertAllRequest.Builder builder;

    @Before
    public void setUp() {
        tableId = TableId.of("test-dataset" ,"test-table");
        builder = InsertAllRequest.newBuilder(tableId);
        sink = new BqSink(bigquery, tableId);
    }

    @Test
    public void ShouldPushMessageToBigQuery() {
        Map<String, Object> user1 = createUser();
        InsertAllRequest request = builder.addRow(user1).build();
        Iterable<Record> records = new ArrayList<>(Arrays.asList(new Record(user1)));
        Status status = sink.push(records);

        verify(bigquery).insertAll(request);
        assertTrue(status.isSuccess());
    }

    private Map<String, Object> createUser() {
        HashMap<String , Object> user = new HashMap<>();
        user.put("name", "alice");
        user.put("age", 24);
        return user;
    }
}