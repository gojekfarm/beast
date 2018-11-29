package com.gojek.beast.sink;

import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.Record;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class QueueSinkTest {

    private Sink<Record> queueSink;

    @Test
    public void shouldPushMessageToQueue() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue);
        List<Record> messages = Collections.singletonList(new Record(new HashMap<>()));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        assertSame(messages, queue.take());
    }

    @Test
    public void shouldPushMultipleMessagesToQueue() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = new LinkedBlockingQueue<>();
        queueSink = new QueueSink(queue);
        List<Record> messages = Arrays.asList(new Record(new HashMap<>()), new Record(new HashMap<>()));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        assertSame(messages, queue.take());
    }

    @Test
    public void shouldReturnFailureStatusOnException() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = mock(BlockingQueue.class);
        List<Record> messages = Arrays.asList(new Record(new HashMap<>()), new Record(new HashMap<>()));
        queueSink = new QueueSink(queue);
        doThrow(new InterruptedException()).when(queue).put(messages);

        Status status = queueSink.push(messages);

        assertFalse(status.isSuccess());
    }
}
