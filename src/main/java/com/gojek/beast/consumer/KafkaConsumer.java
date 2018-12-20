package com.gojek.beast.consumer;

import com.gojek.beast.commiter.KafkaCommitter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
@AllArgsConstructor
public class KafkaConsumer implements KafkaCommitter {
    private final org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public synchronized ConsumerRecords<byte[], byte[]> poll(long timeout) {
        return kafkaConsumer.poll(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("Committing offsets {}", offsets);
        synchronized (kafkaConsumer) {
            kafkaConsumer.commitSync(offsets);
        }
    }

    @Override
    public void wakeup() {
        log.debug("kafka consumer wakeup");
        kafkaConsumer.wakeup();
    }

    public void close() {
        log.debug("Closing kafka consumer");
        kafkaConsumer.wakeup();
        synchronized (kafkaConsumer) {
            kafkaConsumer.close();
        }
    }

}
