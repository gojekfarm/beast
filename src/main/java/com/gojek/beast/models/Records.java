package com.gojek.beast.models;

import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Records implements Iterable<Record> {
    @Delegate
    @Getter
    private final List<Record> records;
    @Getter
    private final Instant polledTime; // time when this batch were fetched or created
    private Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = new HashMap<>();
    private Map<Integer, Long> recordCountByPartition = new HashMap<>();

    public Records(List<Record> records) {
        this(records, Instant.now());
    }

    public Records(List<Record> records, Instant polledTime) {
        this.records = records;
        this.polledTime = polledTime;
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

    public long getSize() {
        return records.stream().mapToLong(Record::getSize).sum();
    }

    public Map<Integer, Long> getRecordCountByPartition() {
        if (recordCountByPartition.isEmpty()) {
            records.forEach(r -> recordCountByPartition.merge(r.getPartition(), 1L, Long::sum));
        }
        return recordCountByPartition;
    }

    @Override
    public String toString() {
        return "Records{"
                + "partitionsCommitOffset=" + getPartitionsCommitOffset()
                + "size=" + records.size()
                + '}';
    }
}
