package com.gojek.beast.sink;

import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.junit.Before;
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
    private QueueConfig queueConfig;
    private Sink queueSink;

    @Before
    public void setUp() throws Exception {
        queueConfig = new QueueConfig(1000);
    }

    @Test
    public void shouldPushMessageToQueue() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue, queueConfig);
        Records messages = new Records(Collections.singletonList(new Record(offsetInfo, new HashMap<>())));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        assertSame(messages, queue.take());
    }

    @Test
    public void shouldPushMultipleMessagesToQueue() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue, queueConfig);
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
        queueSink = new QueueSink(queue, queueConfig);
        doThrow(new InterruptedException()).when(queue).offer(messages, queueConfig.getTimeout(), queueConfig.getTimeoutUnit());

        Status status = queueSink.push(messages);

        assertFalse(status.isSuccess());
    }

    @Test
    public void shouldReturnFailureStatusWhenQueueIsFull() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>(1);
        Records messages = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));
        queue.offer(messages);
        queueSink = new QueueSink(queue, queueConfig);
        Status status = queueSink.push(messages);

        assertFalse(status.isSuccess());
    }
}
