package com.gojek.beast.commiter;

import com.gojek.beast.models.OffsetMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class OffsetAcknowledgerTest {
    private Acknowledger offsetAcknowledger;
    private Set<OffsetMap> acknowledgements;
    @Mock
    private OffsetMap commitPartitionsOffset;

    @Before
    public void setUp() throws Exception {
        acknowledgements = Collections.synchronizedSet(new CopyOnWriteArraySet<>());
        offsetAcknowledger = new OffsetAcknowledger(acknowledgements);
    }

    @Test
    public void shouldPushAcknowledgementsToSet() {
        OffsetMap partition1Offset = mock(OffsetMap.class);

        offsetAcknowledger.acknowledge(commitPartitionsOffset);
        offsetAcknowledger.acknowledge(partition1Offset);

        assertEquals(2, acknowledgements.size());
        assertTrue(acknowledgements.contains(commitPartitionsOffset));
        assertTrue(acknowledgements.contains(partition1Offset));
    }

}
