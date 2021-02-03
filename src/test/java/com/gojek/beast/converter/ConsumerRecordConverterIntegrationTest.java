package com.gojek.beast.converter;

import com.gojek.beast.Clock;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.exception.ErrorWriterFailedException;
import com.gojek.beast.exception.NullInputMessageException;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.dlq.ErrorWriter;
import com.gojek.beast.sink.dlq.WriteStatus;
import com.gojek.beast.util.KafkaConsumerUtil;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.Parser;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.InvalidProtocolBufferException;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerRecordConverterIntegrationTest {
    private ConsumerRecordConverter recordConverter;
    private ConsumerRecordConverter recordConverterWithFailOnDeserializeError;
    private ConsumerRecordConverter recordConverterWithFailOnNull;
    private RowMapper rowMapper;

    private Parser parser;

    private KafkaConsumerUtil util;
    @Mock
    private Clock clock;
    private Long nowEpochMillis;
    @Mock
    private ErrorWriter errorWriter;
    @Mock
    private ErrorWriter errorWriterWithFailedStatus;

    @Before
    public void setUp() {
        when(errorWriter.writeRecords(any())).thenReturn(new SuccessStatus());
        when(errorWriterWithFailedStatus.writeRecords(any())).thenReturn(new WriteStatus(false, Optional.empty()));

        parser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put(1, "bq_order_number");
        columnMapping.put(2, "bq_order_url");
        columnMapping.put(3, "bq_order_details");
        rowMapper = new RowMapper(columnMapping);

        System.setProperty("FAIL_ON_NULL_MESSAGE", "false");
        System.setProperty("FAIL_ON_DESERIALIZE_ERROR", "false");
        recordConverter = new ConsumerRecordConverter(rowMapper, parser, clock,
                ConfigFactory.create(AppConfig.class, System.getProperties()), errorWriter);

        System.setProperty("FAIL_ON_NULL_MESSAGE", "true");
        recordConverterWithFailOnNull = new ConsumerRecordConverter(rowMapper, parser, clock,
                ConfigFactory.create(AppConfig.class, System.getProperties()), errorWriter);

        System.setProperty("FAIL_ON_DESERIALIZE_ERROR", "true");
        recordConverterWithFailOnDeserializeError = new ConsumerRecordConverter(rowMapper, parser, clock,
                ConfigFactory.create(AppConfig.class, System.getProperties()), errorWriter);

        util = new KafkaConsumerUtil();
        nowEpochMillis = Instant.now().toEpochMilli();
        when(clock.currentEpochMillis()).thenReturn(nowEpochMillis);
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));


        Map<Object, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("bq_order_number", "order-2");
        record2ExpectedColumns.put("bq_order_url", "order-url-2");
        record2ExpectedColumns.put("bq_order_details", "order-details-2");
        record2ExpectedColumns.putAll(util.metadataColumns(record2Offset, nowEpochMillis));
        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);

        List<Record> records = recordConverter.convert(messages);

        assertEquals(messages.size(), records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        Map<String, Object> record2Columns = records.get(1).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record2ExpectedColumns.size(), record2Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(record2ExpectedColumns, record2Columns);
    }

    @Test
    public void shouldIgnoreNullRecords() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = util.withOffsetInfo(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);
        List<Record> records = recordConverter.convert(messages);

        assertEquals(1, records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test(expected = NullInputMessageException.class)
    public void shouldThrowExceptionForNullRecords() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = util.withOffsetInfo(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));


        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);
        List<Record> records = recordConverterWithFailOnNull.convert(messages);

        assertEquals(1, records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldWhenNamespaceIsNotProvided() throws InvalidProtocolBufferException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ConsumerRecordConverter recordConverterTest = new ConsumerRecordConverter(rowMapper, parser, clock, appConfig, errorWriter);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1);
        List<Record> records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(appConfig.getBqMetadataNamespace(), "");
    }

    @Test
    public void shouldNamespaceMetadataFieldWhenNamespaceIsProvided() throws InvalidProtocolBufferException {
        System.setProperty("BQ_METADATA_NAMESPACE", "metadata_ns");
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ConsumerRecordConverter recordConverterTest = new ConsumerRecordConverter(rowMapper, parser, clock, appConfig, errorWriter);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.put(appConfig.getBqMetadataNamespace(), util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1);
        List<Record> records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        System.setProperty("BQ_METADATA_NAMESPACE", "");
    }

    @Test
    public void shouldPopulateOffsetInformationForRecord() throws InvalidProtocolBufferException {
        String topic = "order-logs";
        OffsetInfo record1Offset = new OffsetInfo(topic, 1, 100, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo(topic, 2, 200, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> message1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> message2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(message1, message2);

        List<Record> records = recordConverter.convert(messages);

        assertEquals(2, records.size());
        assertEquals(record1Offset, records.get(0).getOffsetInfo());
        assertEquals(record2Offset, records.get(1).getOffsetInfo());
    }

    @Test(expected = InvalidProtocolBufferException.class)
    public void shouldThrowExceptionWhenInvalidRecordsFound() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(record2Offset.getTopic(), record2Offset.getPartition(),
                record2Offset.getOffset(), record2Offset.getTimestamp(), TimestampType.CREATE_TIME, 0,
                0, 0, "invalid-key".getBytes(), "invalid-value".getBytes());

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);
        List<Record> records = recordConverterWithFailOnDeserializeError.convert(messages);

        verify(errorWriter).writeRecords(any());

        assertEquals(1, records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test(expected = ErrorWriterFailedException.class)
    public void shouldThrowExceptionWhenFailedToWriteInDLQ() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(record2Offset.getTopic(), record2Offset.getPartition(),
                record2Offset.getOffset(), record2Offset.getTimestamp(), TimestampType.CREATE_TIME, 0,
                0, 0, "invalid-key".getBytes(), "invalid-value".getBytes());

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);

        System.setProperty("FAIL_ON_NULL_MESSAGE", "false");
        System.setProperty("FAIL_ON_DESERIALIZE_ERROR", "false");
        ConsumerRecordConverter localRecordConverter = new ConsumerRecordConverter(rowMapper, parser, clock,
                ConfigFactory.create(AppConfig.class, System.getProperties()), errorWriterWithFailedStatus);
        List<Record> records = localRecordConverter.convert(messages);

        verify(errorWriterWithFailedStatus).writeRecords(any());

        assertEquals(1, records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldWriteToErrorWriterInvalidRecords() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        ConsumerRecord<byte[], byte[]> record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(record2Offset.getTopic(), record2Offset.getPartition(),
                record2Offset.getOffset(), record2Offset.getTimestamp(), TimestampType.CREATE_TIME, 0,
                0, 0, "invalid-key".getBytes(), "invalid-value".getBytes());

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, nowEpochMillis));

        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);
        List<Record> records = recordConverter.convert(messages);

        verify(errorWriter).writeRecords(any());

        assertEquals(1, records.size());
        Map<String, Object> record1Columns = records.get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }
}
