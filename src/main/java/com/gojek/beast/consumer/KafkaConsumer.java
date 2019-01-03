package com.gojek.beast.consumer;

import com.gojek.beast.commiter.KafkaCommitter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Map;

@Slf4j
public class KafkaConsumer implements KafkaCommitter {
    private final org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> kafkaConsumer;
    @Getter
    private volatile boolean closed;

    public KafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.closed = false;
    }

    public synchronized ConsumerRecords<byte[], byte[]> poll(long timeout) throws WakeupException {
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
        log.info("kafka consumer wakeup");
        kafkaConsumer.wakeup();
        closed = true;
    }

    public void close() {
        if (closed) {
            return;
        }
        log.info("Closing kafka consumer");
        closed = true;
        kafkaConsumer.wakeup();
        synchronized (kafkaConsumer) {
            kafkaConsumer.close();
        }
    }

}
