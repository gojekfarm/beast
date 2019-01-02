package com.gojek.beast.consumer;

import com.gojek.beast.stats.Stats;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final Stats statsClient = Stats.client();

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        statsClient.increment("rebalalancer.partitions.revoked");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        statsClient.increment("rebalalancer.partitions.assigned");
    }
}
