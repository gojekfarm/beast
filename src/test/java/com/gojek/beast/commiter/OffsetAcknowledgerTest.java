package com.gojek.beast.commiter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OffsetAcknowledgerTest {
    private Acknowledger offsetAcknowledger;
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgements;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> commitPartitionsOffset;

    @Before
    public void setUp() throws Exception {
        acknowledgements = Collections.synchronizedSet(new CopyOnWriteArraySet<>());
        offsetAcknowledger = new OffsetAcknowledger(acknowledgements);
    }

    @Test
    public void shouldPushAcknowledgementsToSet() {
        Map<TopicPartition, OffsetAndMetadata> partition1Offset = mock(Map.class);

        offsetAcknowledger.acknowledge(commitPartitionsOffset);
        offsetAcknowledger.acknowledge(partition1Offset);

        assertEquals(2, acknowledgements.size());
        assertTrue(acknowledgements.contains(commitPartitionsOffset));
        assertTrue(acknowledgements.contains(partition1Offset));
    }

}
