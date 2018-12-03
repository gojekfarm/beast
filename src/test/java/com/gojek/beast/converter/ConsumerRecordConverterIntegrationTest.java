package com.gojek.beast.converter;

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
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerRecordConverterIntegrationTest {
    private ConsumerRecordConverter recordConverter;
    private RowMapper rowMapper;

    private Parser parser;

    private KafkaConsumerUtil util;

    @Before
    public void setUp() {
        parser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        ColumnMapping columnMapping = new ColumnMapping();
        columnMapping.put(1, "bq_order_number");
        columnMapping.put(2, "bq_order_url");
        columnMapping.put(3, "bq_order_details");
        rowMapper = new RowMapper(columnMapping);
        recordConverter = new ConsumerRecordConverter(rowMapper, parser);
        util = new KafkaConsumerUtil();
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() throws ParseException {
        ConsumerRecord<byte[], byte[]> record1 = util.createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> record2 = util.createConsumerRecord("order-2", "order-url-2", "order-details-2");

        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");

        Map<Object, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("bq_order_number", "order-2");
        record2ExpectedColumns.put("bq_order_url", "order-url-2");
        record2ExpectedColumns.put("bq_order_details", "order-details-2");
        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(record1, record2);

        List<Record> records = recordConverter.convert(messages);

        assertEquals(messages.size(), records.size());
        assertEquals(record1ExpectedColumns, records.get(0).getColumns());
        assertEquals(record2ExpectedColumns, records.get(1).getColumns());
    }

    @Test
    public void shouldPopulateOffsetInformationForRecord() throws ParseException {
        String topic = "order-logs";
        ConsumerRecord<byte[], byte[]> message1 = util.withTopic(topic).withOffset(100).withPartition(1)
                .createConsumerRecord("order-1", "order-url-1", "order-details-1");
        ConsumerRecord<byte[], byte[]> message2 = util.withTopic(topic).withOffset(200).withPartition(2)
                .createConsumerRecord("order-2", "order-url-2", "order-details-2");
        List<ConsumerRecord<byte[], byte[]>> messages = Arrays.asList(message1, message2);

        List<Record> records = recordConverter.convert(messages);

        assertEquals(2, records.size());
        assertEquals(new OffsetInfo(topic, 1, 100), records.get(0).getOffsetInfo());
        assertEquals(new OffsetInfo(topic, 2, 200), records.get(1).getOffsetInfo());

    }
}
