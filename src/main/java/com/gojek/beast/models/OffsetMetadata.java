package com.gojek.beast.models;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public class OffsetMetadata extends OffsetAndMetadata implements Comparable<OffsetMetadata> {

    public OffsetMetadata(long offset) {
        super(offset);
    }

    @Override
    public int compareTo(OffsetMetadata o) {
        return Long.compare(this.offset(), o.offset());
    }
}
