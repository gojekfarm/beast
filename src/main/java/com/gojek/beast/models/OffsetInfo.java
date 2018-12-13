package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.TopicPartition;

@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
public class OffsetInfo {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;

    public OffsetMetadata getOffsetMetadata() {
        return new OffsetMetadata(offset);
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(topic, partition);
    }
}
