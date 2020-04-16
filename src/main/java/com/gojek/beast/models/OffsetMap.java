package com.gojek.beast.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@AllArgsConstructor
@Getter
public class OffsetMap {
    private Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap;

    public long getSize() {
        return offsetAndMetadataMap.size();
    }

    @Override
    public String toString() {
        return offsetAndMetadataMap.toString();
    }
}
