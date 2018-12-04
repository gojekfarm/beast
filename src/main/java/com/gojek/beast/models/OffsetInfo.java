package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.TopicPartition;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class OffsetInfo {
    private final String topic;
    private final int partition;
    private final long offset;

    public OffsetMetadata getOffsetMetadata() {
        return new OffsetMetadata(offset);
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(topic, partition);
    }
}
