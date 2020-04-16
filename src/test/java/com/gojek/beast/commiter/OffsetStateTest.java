package com.gojek.beast.commiter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetStateTest {
    private CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgements = Collections.synchronizedSet(ackSet);

    @Test
    public void shouldReturnFalseWhenAcknowledgedRecently() {
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, 200, 100);
        state.startTimer();
        state.resetOffset();

        assertFalse(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnFalseAfterFirstRecordAcknowledgement() throws InterruptedException {
        int ackTimeout = 200;
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, ackTimeout, 100);
        state.startTimer();
        Thread.sleep(ackTimeout - 10);

        assertFalse(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnTrueIfNoneAcknowledgedAndTimedOut() throws InterruptedException {
        int ackTimeout = 200;
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, ackTimeout, 100);
        state.startTimer();

        Thread.sleep(ackTimeout + 100);

        assertTrue(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnFalseWhenAcknowledgedWithDifferentOffset() throws InterruptedException {
        int ackTimeout = 200;
        Map<TopicPartition, OffsetAndMetadata> oldOffset = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, ackTimeout, 100);
        state.startTimer();
        state.resetOffset();
        state.resetOffset();

        Thread.sleep(ackTimeout - 20);
        assertFalse(state.shouldCloseConsumer(currOffset));
    }

    @Test
    public void shouldReturnTrueWhenLastAckOffsetIsSameAndTimedOut() throws InterruptedException {
        int ackTimeout = 200;
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, ackTimeout, 100);
        state.startTimer();
        state.resetOffset();

        Thread.sleep(ackTimeout + 10);
        assertTrue(state.shouldCloseConsumer(currOffset));
    }

    @Test
    public void shouldReturnFalseWhenTimerNotStarted() throws InterruptedException {
        int ackTimeout = 200;
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(acknowledgements, ackTimeout, 10);

        Thread.sleep(ackTimeout + 10);
        assertFalse(state.shouldCloseConsumer(currOffset));
    }
}
