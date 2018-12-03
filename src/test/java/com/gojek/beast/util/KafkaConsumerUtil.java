package com.gojek.beast.util;

import com.gojek.beast.TestKey;
import com.gojek.beast.TestMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaConsumerUtil {
    private String topic;
    private int partition;
    private int offset;

    public KafkaConsumerUtil() {
        this.topic = "default-topic";
        this.partition = 1;
        this.offset = 1;
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
        return new ConsumerRecord<>(topic, partition, offset++, key.toByteArray(), message.toByteArray());
    }
}
