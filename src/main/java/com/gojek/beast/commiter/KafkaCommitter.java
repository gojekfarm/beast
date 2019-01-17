package com.gojek.beast.commiter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface KafkaCommitter {
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    // Use wakeup to throw exception and consumer handles it and closes the loop
    void wakeup(String reason);
}
