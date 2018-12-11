package com.gojek.beast.sink;

import com.gojek.beast.TestKey;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.Converter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.parser.ProtoParser;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.protobuf.Timestamp;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqIntegrationTest {
    @Captor
    private ArgumentCaptor<InsertAllRequest> insertRequestCaptor;
    @Mock
    private MockLowLevelHttpResponse response;
    private BigQuery bigQuery;
    //{0=[BigQueryError{reason=invalid, location=age, message=no such field.}]}
    private Converter converter;
    @Mock
    private InsertAllResponse successfulResponse;
    @Mock
    private BigQuery bigQueryMock;

    @Before
    public void setUp() throws Exception {
        bigQuery = authenticatedBQ();
    }

    public BigQuery authenticatedBQ() {
        GoogleCredentials credentials = null;
        File credentialsPath = new File("credentials.json  # Replace, using a regex");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build().getService();
    }

    public MockHttpTransport getTransporter(final MockLowLevelHttpResponse httpResponse) {
        return new MockHttpTransport.Builder()
                .setLowLevelHttpRequest(
                        new MockLowLevelHttpRequest() {
                            @Override
                            public LowLevelHttpResponse execute() throws IOException {
                                return httpResponse;
                            }
                        })
                .build();
    }


    @Ignore
    @Test
    public void shouldPushMessagesToBqActual() {
        TableId tableId = TableId.of("bqsinktest", "users");
        BqSink bqSink = new BqSink(bigQuery, tableId);

        HashMap<String, Object> columns = new HashMap<>();
        columns.put("name", "alice");
        columns.put("aga", 25);
        columns.put("location", 25123);
        columns.put("created_at", new DateTime(new Date()));

        Status push = bqSink.push(new Records(Arrays.asList(new Record(new OffsetInfo("default-topic", 0, 0), columns))));

        assertTrue(push.isSuccess());
    }

    @Test
    public void shouldParseAndPushMessagesToBq() throws Exception {
        TableId tableId = TableId.of("bqsinktest", "test_messages");
        BqSink bqSink = new BqSink(bigQueryMock, tableId);
        String orderNumber = "order-1";
        String orderUrl = "order_url";
        String orderDetails = "order_details";
        Instant now = Instant.now();
        long second = now.getEpochSecond();
        int nano = now.getNano();
        ColumnMapping mapping = new ColumnMapping();
        mapping.put(1, "order_number");
        mapping.put(2, "order_url");
        mapping.put(3, "order_details");
        mapping.put(4, "created_at");

        converter = new ConsumerRecordConverter(new RowMapper(mapping), new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName()));

        Timestamp createdAt = Timestamp.newBuilder().setSeconds(second).setNanos(nano).build();
        TestKey key = TestKey.newBuilder().setOrderNumber(orderNumber).setOrderUrl(orderUrl).build();
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .setCreatedAt(createdAt)
                .build();
        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(new ConsumerRecord<>("topic", 1, 1, key.toByteArray(), message.toByteArray()));
        when(successfulResponse.hasErrors()).thenReturn(false);
        when(bigQueryMock.insertAll(insertRequestCaptor.capture())).thenReturn(successfulResponse);

        List<Record> records = converter.convert(messages);
        assertTrue(bqSink.push(new Records(records)).isSuccess());

        List<InsertAllRequest.RowToInsert> bqRows = insertRequestCaptor.getValue().getRows();
        assertEquals(1, bqRows.size());
        Map<String, Object> contents = bqRows.get(0).getContent();
        assertEquals("should have 4 columns in record", 4, contents.size());
        assertEquals(orderUrl, contents.get("order_url"));
        assertEquals(orderNumber, contents.get("order_number"));
        assertEquals(orderDetails, contents.get("order_details"));
        assertEquals(new DateTime(Instant.ofEpochSecond(second, nano).toEpochMilli()), contents.get("created_at"));
    }
}
