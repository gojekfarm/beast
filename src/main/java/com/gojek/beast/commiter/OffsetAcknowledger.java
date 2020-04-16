package com.gojek.beast.commiter;

import com.gojek.beast.models.OffsetMap;
import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@AllArgsConstructor
public class OffsetAcknowledger implements Acknowledger {
    private final Stats statsClient = Stats.client();
    private Set<OffsetMap> partitionOffsetAck;

    @Override
    public boolean acknowledge(OffsetMap offsets) {
        boolean status = partitionOffsetAck.add(offsets);
        statsClient.gauge("queue.elements,name=ack", partitionOffsetAck.size());
        log.debug("Acknowledged by bq sink: {} status: {}", offsets, status);
        return status;
    }

    @Override
    public void close(String reason) {
        partitionOffsetAck.clear();
    }
}
