package com.gojek.beast.sink;

import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class QueueSinkTest {

    private final OffsetInfo offsetInfo = new OffsetInfo("default-topic", 0, 0, Instant.now().toEpochMilli());
    private Sink queueSink;

    @Test
    public void shouldPushMessageToQueue() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue);
        Records messages = new Records(Collections.singletonList(new Record(offsetInfo, new HashMap<>())));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        assertSame(messages, queue.take());
    }

    @Test
    public void shouldPushMultipleMessagesToQueue() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue);
        Records messages = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        assertSame(messages, queue.take());
    }

    @Test
    public void shouldReturnFailureStatusOnException() throws InterruptedException {
        BlockingQueue<Records> queue = mock(BlockingQueue.class);
        Records messages = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));
        queueSink = new QueueSink(queue);
        doThrow(new InterruptedException()).when(queue).put(messages);

        Status status = queueSink.push(messages);

        assertFalse(status.isSuccess());
    }
}
