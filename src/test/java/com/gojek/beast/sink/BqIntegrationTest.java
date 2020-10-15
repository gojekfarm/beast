package com.gojek.beast.sink;

import com.gojek.beast.*;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.Converter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.sink.bq.BQRowWithoutId;
import com.gojek.beast.sink.bq.BaseBQTest;
import com.gojek.beast.sink.bq.handler.BQRow;
import com.gojek.beast.sink.bq.handler.gcs.GCSErrorWriter;
import com.gojek.beast.sink.bq.handler.BQErrorHandler;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.sink.bq.handler.BQResponseParser;
import com.gojek.beast.sink.bq.handler.ErrorWriter;
import com.gojek.beast.sink.bq.handler.impl.OOBErrorHandler;
import com.gojek.beast.util.ProtoUtil;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.DateTime;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqIntegrationTest extends BaseBQTest {
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
    @Mock
    private Clock clock;
    @Mock
    private Storage gcsStoreMock;
    @Mock
    private Blob blobMock;
    private long nowMillis;
    private BQErrorHandler gcsSinkHandler;
    private BQRow bqRow;
    private String gcsBucket = "test-integ-godata-dlq-beast";
    private AppConfig appConfig;

    @Before
    public void setUp() throws Exception {
        nowMillis = Instant.now().toEpochMilli();
        when(clock.currentEpochMillis()).thenReturn(nowMillis);
        bqRow = new BQRowWithoutId();
        ErrorWriter errorWriter = new GCSErrorWriter(gcsStoreMock, "test-integ-godata", "test-integ-godata-dlq/beast");
        gcsSinkHandler = new OOBErrorHandler(errorWriter);
        final BlobId blobId = BlobId.of("test-integ-godata", "test-integ-godata-dlq/beast/testfile");
        final BlobInfo objectInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
    }

    @Ignore
    @Test
    public void shouldPushNestedMessage() {
        Instant now = Instant.now();
        long second = now.getEpochSecond();
        int nano = now.getNano();
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(second).setNanos(nano).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(com.gojek.beast.Status.COMPLETED)
                .build();
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestNestedMessage.class.getName());
        TestNestedMessage nestedMsg = TestNestedMessage.newBuilder()
                .setSingleMessage(testMessage)
                .setNestedId("nested-id")
                .build();
        TableId tableId = TableId.of("bqsinktest", "test_nested_messages");
        BqSink bqSink = new BqSink(authenticatedBQ(), tableId, new BQResponseParser(), gcsSinkHandler, bqRow);


        OffsetInfo offsetInfo = new OffsetInfo("topic", 1, 1, Instant.now().toEpochMilli());
        Map<String, Object> columns = new HashMap<>();
        HashMap<String, Object> nested = new HashMap<>();
        nested.put("order_number", nestedMsg.getSingleMessage().getOrderNumber());
        nested.put("order_url", nestedMsg.getSingleMessage().getOrderUrl());

        columns.put("id", nestedMsg.getNestedId());
        columns.put("msg", nested);

        Status push = bqSink.push(new Records(Arrays.asList(new Record(offsetInfo, columns))));
        assertTrue(push.isSuccess());
    }

    @Ignore
    @Test
    public void shouldPushTestNestedRepeatedMessages() throws InvalidProtocolBufferException {
        Instant now = Instant.now();
        long second = now.getEpochSecond();
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestNestedRepeatedMessage.class.getName());
        TestNestedRepeatedMessage protoMessage = TestNestedRepeatedMessage.newBuilder()
                .addRepeatedMessage(ProtoUtil.generateTestMessage(now))
                .addRepeatedMessage(ProtoUtil.generateTestMessage(now))
                .build();

        TableId tableId = TableId.of("bqsinktest", "nested_messages");
        BqSink bqSink = new BqSink(authenticatedBQ(), tableId, new BQResponseParser(), gcsSinkHandler, bqRow);

        ColumnMapping columnMapping = new ColumnMapping();
        ColumnMapping nested = new ColumnMapping();
        nested.put("record_name", "messsages");
        nested.put("1", "order_number");
        nested.put("2", "order_url");
        columnMapping.put("2", nested);
        ConsumerRecordConverter customConverter = new ConsumerRecordConverter(new RowMapper(columnMapping), protoParser, clock, appConfig);


        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 1, second, TimestampType.CREATE_TIME,
                0, 0, 1, null, protoMessage.toByteArray());

        List<Record> records = customConverter.convert(Collections.singleton(consumerRecord));
        Status push = bqSink.push(new Records(records));
        assertTrue(push.isSuccess());
    }

    @Ignore
    @Test
    public void shouldPushMessagesToBqActual() {
        TableId tableId = TableId.of("bqsinktest", "users");
        BqSink bqSink = new BqSink(authenticatedBQ(), tableId, new BQResponseParser(), gcsSinkHandler, bqRow);

        HashMap<String, Object> columns = new HashMap<>();
        columns.put("name", "someone_else");
        columns.put("age", 26);
        columns.put("location", 15123);
        columns.put("created_at", new DateTime(new Date()));

        HashMap<String, Object> route1 = new HashMap<>();
        route1.put("name", "route1");
        route1.put("position", "1");
        HashMap<String, Object> route2 = new HashMap<>();
        route2.put("name", "route2");
        route2.put("position", "2");

        columns.put("routes", Arrays.asList(route1, route2));

        Status push = bqSink.push(new Records(Arrays.asList(new Record(new OffsetInfo("default-topic", 0, 0, Instant.now().toEpochMilli()), columns))));

        assertTrue(push.isSuccess());
    }

    @Ignore
    @Test
    public void testBQErrorsAreStoredInGCS() throws InvalidProtocolBufferException {
        Instant validNow = Instant.now();
        TestMessage validMessage1 = getTestMessage("VALID-11-testValidMessageInBQ", validNow);
        TestMessage validMessage2 = getTestMessage("VALID-12-testValidMessageInBQ", validNow);

        Instant inValidLater = Instant.now().plus(Duration.ofDays(185));
        Instant inValidBefore = Instant.now().minus(Duration.ofDays(365));
        TestMessage inValidMessage1 = getTestMessage("INVALID-21-testBQErrorsAreStoredInGCS", inValidLater);
        TestMessage inValidMessage2 = getTestMessage("INVALID-21-testBQErrorsAreStoredInGCS", inValidLater);

        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        columnMapping.put("2", "order_url");
        columnMapping.put("3", "order_details");
        columnMapping.put("4", "created_at");

        List<Record> validRecords = getKafkaConsumerRecords(columnMapping, validNow, "testBQErrorsAreStoredInGCS-valid", 1,
                1L, clock, validMessage1, validMessage2);
        List<Record> inValidRecords = getKafkaConsumerRecords(columnMapping, validNow, "testBQErrorsAreStoredInGCS-valid", 2,
                10L, clock, inValidMessage1, inValidMessage2);

        List<Record> allRecords = new ArrayList<>();
        allRecords.addAll(validRecords);
        allRecords.addAll(inValidRecords);

        final Storage gcsStore = authenticatedGCStorageInstance();

        //Insert into BQ
        TableId tableId = TableId.of("playground", "test_nested_messages");
        BQErrorHandler errorHandler = new OOBErrorHandler(new GCSErrorWriter(gcsStore, gcsBucket, "test-integ-beast"));
        BqSink bqSink = new BqSink(authenticatedBQ(), tableId, new BQResponseParser(), errorHandler, bqRow);
        Status push = bqSink.push(new Records(allRecords));
        assertTrue("Invalid Message should have been inserted into GCS Sink and success status should be true", push.isSuccess());
    }

    @Ignore
    @Test
    public void testBQValidOnlyMessagesAreStoredInBQ() throws InvalidProtocolBufferException {
        Instant validNow = Instant.now();
        TestMessage validMessage = getTestMessage("testBQValidOnlyMessagesAreStoredInBQ-valid-" + System.currentTimeMillis(), validNow);

        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put("1", "order_number");
        columnMapping.put("2", "order_url");
        columnMapping.put("3", "order_details");
        columnMapping.put("4", "created_at");

        List<Record> validRecords = getKafkaConsumerRecords(columnMapping, validNow, "testBQErrorsAreStoredInGCS-valid", 1,
                1L, clock, validMessage);

        final Storage gcsStore = authenticatedGCStorageInstance();

        //Insert into BQ
        TableId tableId = TableId.of("playground", "test_nested_messages");
        BQErrorHandler errorHandler = new OOBErrorHandler(new GCSErrorWriter(gcsStore, gcsBucket, "test-integ-beast"));
        BqSink bqSink = new BqSink(authenticatedBQ(), tableId, new BQResponseParser(), errorHandler, bqRow);
        Status push = bqSink.push(new Records(validRecords));
        assertTrue("Invalid Message should have been inserted into GCS Sink and success status should be true", push.isSuccess());
    }

    @Test
    public void shouldParseAndPushMessagesToBq() throws Exception {
        TableId tableId = TableId.of("bqsinktest", "test_messages");
        BqSink bqSink = new BqSink(bigQueryMock, tableId, new BQResponseParser(), gcsSinkHandler, bqRow);
        String orderNumber = "order-1";
        String orderUrl = "order_url";
        String orderDetails = "order_details";
        Instant now = Instant.now();
        long second = now.getEpochSecond();
        int nano = now.getNano();
        ColumnMapping mapping = new ColumnMapping();
        mapping.put("1", "order_number");
        mapping.put("2", "order_url");
        mapping.put("3", "order_details");
        mapping.put("4", "created_at");
        mapping.put("5", "status");
        mapping.put("6", "discounted_value");
        mapping.put("7", "success");
        mapping.put("8", "order_price");
        mapping.put("12", "aliases");

        ColumnMapping currStateMapping = new ColumnMapping();
        currStateMapping.put("record_name", "current_state");
        currStateMapping.put("1", "key");
        currStateMapping.put("2", "value");
        mapping.put("9", currStateMapping);

        converter = new ConsumerRecordConverter(new RowMapper(mapping), new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName()), clock, appConfig);
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(second).setNanos(nano).build();
        TestKey key = TestKey.newBuilder().setOrderNumber(orderNumber).setOrderUrl(orderUrl).build();
        com.gojek.beast.Status completed = com.gojek.beast.Status.COMPLETED;
        long discount = 1234;
        float price = 1234.5678f;
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .setCreatedAt(createdAt)
                .setStatus(completed)
                .setDiscount(discount)
                .setPrice(price)
                .setSuccess(true)
                .addAliases("alias1").addAliases("alias2")
                .putCurrentState("state_key_1", "state_value_1")
                .putCurrentState("state_key_2", "state_value_2")
                .build();
        String topic = "topic";
        int partition = 1, offset = 1;
        long recordTimestamp = Instant.now().toEpochMilli();
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(topic, partition, offset, recordTimestamp, TimestampType.CREATE_TIME,
                0, 0, 1, key.toByteArray(), message.toByteArray());

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(consumerRecord);
        when(successfulResponse.hasErrors()).thenReturn(false);
        when(bigQueryMock.insertAll(insertRequestCaptor.capture())).thenReturn(successfulResponse);

        List<Record> records = converter.convert(messages);
        Status status = bqSink.push(new Records(records));
        assertTrue(status.isSuccess());

        List<InsertAllRequest.RowToInsert> bqRows = insertRequestCaptor.getValue().getRows();
        assertEquals(1, bqRows.size());
        Map<String, Object> contents = bqRows.get(0).getContent();
        assertEquals("should have same number of columns as mappings, with metadata columns", mapping.size() + 5, contents.size());
        assertEquals(orderUrl, contents.get("order_url"));
        assertEquals(orderNumber, contents.get("order_number"));
        assertEquals(orderDetails, contents.get("order_details"));
        assertEquals(new DateTime(Instant.ofEpochSecond(second, nano).toEpochMilli()), contents.get("created_at"));
        assertEquals(completed.toString(), contents.get("status"));
        assertEquals(discount, contents.get("discounted_value"));
        assertEquals(price, contents.get("order_price"));
        assertEquals(Arrays.asList("alias1", "alias2"), contents.get("aliases"));
        assertTrue(Boolean.valueOf(contents.get("success").toString()));
        containsMetadata(contents, new OffsetInfo(topic, partition, offset, recordTimestamp));
        List repeatedStateMap = (List) contents.get("current_state");
        assertEquals("state_key_1", ((Map) repeatedStateMap.get(0)).get("key"));
        assertEquals("state_value_1", ((Map) repeatedStateMap.get(0)).get("value"));
        assertEquals("state_key_2", ((Map) repeatedStateMap.get(1)).get("key"));
        assertEquals("state_value_2", ((Map) repeatedStateMap.get(1)).get("value"));
    }

    private void containsMetadata(Map<String, Object> columns, OffsetInfo offsetInfo) {
        assertEquals("partition metadata mismatch", columns.get("message_partition"), offsetInfo.getPartition());
        assertEquals("offset metadata mismatch", columns.get("message_offset"), offsetInfo.getOffset());
        assertEquals("topic metadata mismatch", columns.get("message_topic"), offsetInfo.getTopic());
        assertEquals("message timestamp metadata mismatch", columns.get("message_timestamp"), new DateTime(offsetInfo.getTimestamp()));
        assertEquals("load time metadata mismatch", columns.get("load_time"), new DateTime(nowMillis));
    }

}
