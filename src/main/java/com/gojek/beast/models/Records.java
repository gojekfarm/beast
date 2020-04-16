package com.gojek.beast.models;

import com.gojek.beast.sink.SinkElement;
import lombok.Getter;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Records implements Iterable<Record>, SinkElement {
    @Delegate
    @Getter
    private final List<Record> records;
    @Getter
    private final Instant polledTime; // time when this batch were fetched or created
    private Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = new HashMap<>();

    public Records(List<Record> records) {
        this(records, Instant.now());
    }

    public Records(List<Record> records, Instant polledTime) {
        this.records = records;
        this.polledTime = polledTime;
    }

    public OffsetMap getPartitionsCommitOffset() {
        // kafka commit requires offset + 1 (next offset)
        if (!partitionsCommitOffset.isEmpty()) {
            return new OffsetMap(partitionsCommitOffset);
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
        return new OffsetMap(partitionsCommitOffset);
    }

    public long getSize() {
        return records.stream().mapToLong(record -> {
            return record.getSize();
        }).sum();
    }

    @Override
    public String toString() {
        return "Records{"
                + "partitionsCommitOffset=" + getPartitionsCommitOffset()
                + "size=" + records.size()
                + '}';
    }
}
