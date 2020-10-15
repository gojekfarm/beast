package com.gojek.beast.util;

import com.gojek.beast.TestKey;
import com.gojek.beast.TestMessage;
import com.gojek.beast.models.OffsetInfo;
import com.google.api.client.util.DateTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class KafkaConsumerUtil {
    private long timestamp;
    private String topic;
    private int partition;
    private long offset;

    public KafkaConsumerUtil() {
        this.topic = "default-topic";
        this.partition = 1;
        this.offset = 1;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public KafkaConsumerUtil withOffset(int value) {
        offset = value;
        return this;
    }

    public KafkaConsumerUtil withPartition(int value) {
        partition = value;
        return this;
    }

    public KafkaConsumerUtil withTopic(String value) {
        topic = value;
        return this;
    }

    public KafkaConsumerUtil withOffsetInfo(OffsetInfo offsetInfo) {
        this.topic = offsetInfo.getTopic();
        this.partition = offsetInfo.getPartition();
        this.offset = offsetInfo.getOffset();
        this.timestamp = offsetInfo.getTimestamp();
        return this;
    }

    public ConsumerRecord<byte[], byte[]> createConsumerRecord(String orderNumber, String orderUrl, String orderDetails) {
        TestKey key = TestKey.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .setOrderDetails(orderDetails)
                .build();
        return new ConsumerRecord<>(topic, partition, offset++, timestamp, TimestampType.CREATE_TIME, 0, 0, 0, key.toByteArray(), message.toByteArray());
    }

    public ConsumerRecord<byte[], byte[]> createEmptyValueConsumerRecord(String orderNumber, String orderUrl) {
        TestKey key = TestKey.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl(orderUrl)
                .build();
        return new ConsumerRecord<>(topic, partition, offset++, timestamp, TimestampType.CREATE_TIME, 0, 0, 0, key.toByteArray(), null);
    }

    public Map<String, Object> metadataColumns(OffsetInfo offsetInfo, long epochMillis) {
        Map<String, Object> metadataColumns = new HashMap<>();
        metadataColumns.put("message_partition", offsetInfo.getPartition());
        metadataColumns.put("message_offset", offsetInfo.getOffset());
        metadataColumns.put("message_topic", offsetInfo.getTopic());
        metadataColumns.put("message_timestamp", new DateTime(offsetInfo.getTimestamp()));
        metadataColumns.put("load_time", new DateTime(epochMillis));
        return metadataColumns;
    }

    public void assertMetadata(Map<String, Object> recordColumns, OffsetInfo offsetInfo, long nowEpochMillis) {
        assertEquals("partition metadata mismatch", recordColumns.get("message_partition"), offsetInfo.getPartition());
        assertEquals("offset metadata mismatch", recordColumns.get("message_offset"), offsetInfo.getOffset());
        assertEquals("topic metadata mismatch", recordColumns.get("message_topic"), offsetInfo.getTopic());
        assertEquals("message timestamp metadata mismatch", recordColumns.get("message_timestamp"), new DateTime(offsetInfo.getTimestamp()));
        assertEquals("load time metadata mismatch", recordColumns.get("load_time"), new DateTime(nowEpochMillis));
    }
}
