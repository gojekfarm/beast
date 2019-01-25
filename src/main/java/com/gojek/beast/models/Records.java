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
    private Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = new HashMap<>();

    public Records(List<Record> records) {
        this.records = records;
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionsCommitOffset() {
        // kafka commit requires offset + 1 (next offset)
        if (!partitionsCommitOffset.isEmpty()) {
            return partitionsCommitOffset;
        }
        records.forEach(r -> {
            OffsetInfo offsetInfo = r.getOffsetInfo();
            TopicPartition key = offsetInfo.getTopicPartition();
            OffsetMetadata value = new OffsetMetadata(offsetInfo.getOffset() + 1);
            OffsetMetadata previousOffset = (OffsetMetadata) partitionsCommitOffset.getOrDefault(key, new OffsetMetadata(Integer.MIN_VALUE));
            if (previousOffset.compareTo(value) < 0) {
                partitionsCommitOffset.put(key, value);
            }
        });
        return partitionsCommitOffset;
    }

    @Override
    public String toString() {
        return "Records{"
                + "partitionsCommitOffset=" + getPartitionsCommitOffset()
                + "size=" + records.size()
                + '}';
    }
}
