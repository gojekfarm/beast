package com.gojek.beast.commiter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface Committer {
    boolean acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets);

    void close(String reason);
}
