package com.gojek.beast.converter;

import com.gojek.beast.Clock;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.models.Record;
import com.gojek.beast.parser.Parser;
import com.gojek.beast.parser.ProtoParser;
import com.gojek.beast.util.KafkaConsumerUtil;
import com.gojek.de.stencil.StencilClientFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerRecordConverterIntegrationTest {
    private ConsumerRecordConverter recordConverter;
    private RowMapper rowMapper;

    private Parser parser;

    private KafkaConsumerUtil util;
    private int totalMetadataColumns;
    @Mock
    private Clock clock;
    private Long nowEpochMillis;

    @Before
    public void setUp() {
        parser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put(1, "bq_order_number");
        columnMapping.put(2, "bq_order_url");
        columnMapping.put(3, "bq_order_details");
        rowMapper = new RowMapper(columnMapping);
        recordConverter = new ConsumerRecordConverter(rowMapper, parser, clock);
        util = new KafkaConsumerUtil();
        totalMetadataColumns = 5;
        nowEpochMillis = Instant.now().toEpochMilli();
        when(clock.currentEpochMillis()).thenReturn(nowEpochMillis);
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() throws ParseException {
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
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(record2ExpectedColumns, record2Columns);
    }

    @Test
    public void shouldPopulateOffsetInformationForRecord() throws ParseException {
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
}
