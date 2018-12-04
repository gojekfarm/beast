package com.gojek.beast.models;

import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Records implements Iterable<Record> {
    @Delegate
    @Getter
    private final List<Record> records;
    private Map<TopicPartition, OffsetAndMetadata> maxPartitionOffsets = new HashMap<>();

    public Records(List<Record> records) {
        this.records = records;
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionsMaxOffset() {
        if (!maxPartitionOffsets.isEmpty()) {
            return maxPartitionOffsets;
        }
        records.forEach(r -> {
            OffsetInfo offsetInfo = r.getOffsetInfo();
            TopicPartition key = offsetInfo.getTopicPartition();
            OffsetMetadata value = offsetInfo.getOffsetMetadata();
            OffsetMetadata previousOffset = (OffsetMetadata) maxPartitionOffsets.getOrDefault(key, new OffsetMetadata(Integer.MIN_VALUE));
            if (previousOffset.compareTo(value) < 0) {
                maxPartitionOffsets.put(key, value);
            }
        });
        return maxPartitionOffsets;
    }
}
