package com.gojek.beast.commiter;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommitterTest {
    private OffsetCommitter offsetCommitter;

    @Mock
    private Records records;
    private Queue<Records> commitQ;

    @Before
    public void setUp() throws Exception {
        commitQ = new LinkedBlockingQueue<>();
        offsetCommitter = new OffsetCommitter(commitQ);
    }

    @Test
    public void shouldPushIncomingRecordsToCommitQueue() throws InterruptedException {
        Status status = offsetCommitter.push(records);

        assertEquals(1, commitQ.size());
        assertEquals(records, commitQ.poll());
        assertTrue(status.isSuccess());
    }
}
