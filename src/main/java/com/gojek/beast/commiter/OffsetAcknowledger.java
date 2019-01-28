package com.gojek.beast.commiter;

import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

@AllArgsConstructor
public class OffsetAcknowledger implements Acknowledger {
    private final Stats statsClient = Stats.client();
    private Set<Map<TopicPartition, OffsetAndMetadata>> partitionOffsetAck;

    @Override
    public boolean acknowledge(Map<TopicPartition, OffsetAndMetadata> offsets) {
        boolean status = partitionOffsetAck.add(offsets);
        statsClient.gauge("queue.elements,name=ack", partitionOffsetAck.size());
        return status;
    }

    @Override
    public void close(String reason) {
        partitionOffsetAck.clear();
    }
}
