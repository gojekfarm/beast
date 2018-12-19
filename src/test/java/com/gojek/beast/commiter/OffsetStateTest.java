package com.gojek.beast.commiter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetStateTest {


    @Test
    public void shouldReturnFalseWhenAcknowledgedRecently() {
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(10000);
        state.startTimer();
        state.resetOffset(offset);

        assertFalse(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnFalseAfterFirstRecordAcknowledgement() throws InterruptedException {
        int ackTimeout = 100;
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(ackTimeout);
        state.startTimer();
        Thread.sleep(ackTimeout - 10);

        assertFalse(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnTrueIfNoneAcknowledgedAndTimedOut() throws InterruptedException {
        int ackTimeout = 70;
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        offset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(ackTimeout);
        state.startTimer();

        Thread.sleep(ackTimeout + 100);

        assertTrue(state.shouldCloseConsumer(offset));
    }

    @Test
    public void shouldReturnFalseWhenAcknowledgedWithDifferentOffset() throws InterruptedException {
        int ackTimeout = 100;
        Map<TopicPartition, OffsetAndMetadata> oldOffset = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(ackTimeout);
        state.startTimer();
        state.resetOffset(oldOffset);
        state.resetOffset(currOffset);

        Thread.sleep(ackTimeout - 20);
        assertFalse(state.shouldCloseConsumer(currOffset));
    }

    @Test
    public void shouldReturnTrueWhenLastAckOffsetIsSameAndTimedOut() throws InterruptedException {
        int ackTimeout = 100;
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(ackTimeout);
        state.startTimer();
        state.resetOffset(currOffset);

        Thread.sleep(ackTimeout + 10);
        assertTrue(state.shouldCloseConsumer(currOffset));
    }

    @Test
    public void shouldReturnFalseWhenTimerNotStarted() throws InterruptedException {
        int ackTimeout = 10;
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        currOffset.put(new TopicPartition("topic", 1), new OffsetAndMetadata(101));
        OffsetState state = new OffsetState(ackTimeout);

        Thread.sleep(ackTimeout + 10);
        assertFalse(state.shouldCloseConsumer(currOffset));
    }
}
