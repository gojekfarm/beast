package com.gojek.beast.sink;

import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OffsetMapQueueSinkTest {

    private final OffsetInfo offsetInfo = new OffsetInfo("default-topic", 0, 0, Instant.now().toEpochMilli());
    private QueueConfig queueConfig;
    private Sink queueSink;

    @Before
    public void setUp() throws Exception {
        queueConfig = new QueueConfig(1000);
    }

    @Test
    public void shouldPushMessageToQueue() throws InterruptedException {
        BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> queue = new LinkedBlockingQueue<>();
        queueSink = new OffsetMapQueueSink(queue, queueConfig);
        Records messages = new Records(Collections.singletonList(new Record(offsetInfo, new HashMap<>())));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = queue.take();
        assertEquals(1, partitionsCommitOffset.size());
        Map.Entry<TopicPartition, OffsetAndMetadata> offset = partitionsCommitOffset.entrySet().iterator().next();
        assertEquals(offset.getKey().topic(), "default-topic");
        assertEquals(offset.getKey().partition(), 0);
        assertEquals(offset.getValue().offset(), 1);
    }

    @Test
    public void shouldPushMultipleMessagesToQueue() throws InterruptedException {
        BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> queue = new LinkedBlockingQueue<>();
        queueSink = new OffsetMapQueueSink(queue, queueConfig);
        Records messages = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));

        Status status = queueSink.push(messages);

        assertTrue(status.isSuccess());
        assertEquals(1, queue.size());
        Map<TopicPartition, OffsetAndMetadata> partitionsCommitOffset = queue.take();
        assertEquals(1, partitionsCommitOffset.size());
        Map.Entry<TopicPartition, OffsetAndMetadata> offset = partitionsCommitOffset.entrySet().iterator().next();
        assertEquals(offset.getKey().topic(), "default-topic");
        assertEquals(offset.getKey().partition(), 0);
        assertEquals(offset.getValue().offset(), 1);
    }

    @Test
    public void shouldReturnFailureStatusOnException() throws InterruptedException {
        BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> queue = mock(BlockingQueue.class);
        Records records = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));
        queueSink = new OffsetMapQueueSink(queue, queueConfig);
        Map<TopicPartition, OffsetAndMetadata> offsetMap = records.getPartitionsCommitOffset();
        doThrow(new InterruptedException()).when(queue).offer(offsetMap, queueConfig.getTimeout(), queueConfig.getTimeoutUnit());

        Status status = queueSink.push(records);

        assertFalse(status.isSuccess());
    }

    @Test
    public void shouldReturnFailureStatusWhenQueueIsFull() {
        BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> queue = new LinkedBlockingQueue<>(1);
        Records records = new Records(Arrays.asList(new Record(offsetInfo, new HashMap<>()), new Record(offsetInfo, new HashMap<>())));
        queue.offer(records.getPartitionsCommitOffset());
        queueSink = new OffsetMapQueueSink(queue, queueConfig);
        Status status = queueSink.push(records);

        assertFalse(status.isSuccess());
    }
}
