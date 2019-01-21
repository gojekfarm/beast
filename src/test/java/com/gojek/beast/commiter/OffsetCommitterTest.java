package com.gojek.beast.commiter;

import com.gojek.beast.consumer.KafkaConsumer;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import static com.gojek.beast.util.WorkerUtil.closeWorker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommitterTest {

    @Mock
    private Records records;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> commitPartitionsOffset;
    @Mock
    private KafkaConsumer kafkaConsumer;
    private BlockingQueue<Records> commitQ;
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgements;
    private int acknowledgeTimeoutMs;
    private OffsetCommitter offsetCommitter;
    @Mock
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgeSetMock;
    @Mock
    private OffsetState offsetState;

    @Before
    public void setUp() {
        commitQ = new LinkedBlockingQueue<>();
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
        acknowledgements = Collections.synchronizedSet(ackSet);
        when(offsetState.shouldCloseConsumer(any())).thenReturn(false);
        offsetCommitter = new OffsetCommitter(commitQ, acknowledgements, kafkaConsumer, offsetState);
        acknowledgeTimeoutMs = 15000;
    }

    @After
    public void tearDown() throws Exception {
        commitQ.clear();
        acknowledgements.clear();
        offsetCommitter.stop("some reason");
    }

    @Test
    public void shouldPushIncomingRecordsToCommitQueue() {
        Status status = offsetCommitter.push(records);

        assertEquals(1, commitQ.size());
        assertEquals(records, commitQ.poll());
        assertTrue(status.isSuccess());
    }

    @Test
    public void shouldPushAcknowledgementsToSet() {
        Map<TopicPartition, OffsetAndMetadata> partition1Offset = mock(Map.class);

        offsetCommitter.acknowledge(commitPartitionsOffset);
        offsetCommitter.acknowledge(partition1Offset);

        assertEquals(2, acknowledgements.size());
        assertTrue(acknowledgements.contains(commitPartitionsOffset));
        assertTrue(acknowledgements.contains(partition1Offset));
    }

    @Ignore
    @Test
    public void shouldCommitFirstOffsetWhenAcknowledged() {
        when(records.getPartitionsCommitOffset()).thenReturn(commitPartitionsOffset);
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = spy(new CopyOnWriteArraySet<>());
        Set<Map<TopicPartition, OffsetAndMetadata>> acks = Collections.synchronizedSet(ackSet);
        BlockingQueue<Records> commitQueue = spy(new LinkedBlockingQueue<Records>());
        OffsetCommitter committer = new OffsetCommitter(commitQueue, acks, kafkaConsumer, offsetState);
        committer.setDefaultSleepMs(10);
        committer.push(records);
        committer.acknowledge(commitPartitionsOffset);

        new Thread(committer).start();

        closeWorker(committer, 500);
        verify(commitQueue, atLeast(1)).peek();

        InOrder callOrder = inOrder(kafkaConsumer, records);
        callOrder.verify(records, atLeast(1)).getPartitionsCommitOffset();
        callOrder.verify(kafkaConsumer, times(1)).commitSync(commitPartitionsOffset);
        assertTrue(commitQueue.isEmpty());
        assertTrue(acks.isEmpty());
    }

    @Test
    public void shouldBlockPushingWhenQueueIsFull() throws InterruptedException {
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
        Set<Map<TopicPartition, OffsetAndMetadata>> acks = Collections.synchronizedSet(ackSet);
        LinkedBlockingQueue<Records> commitQueue = new LinkedBlockingQueue<>(1);
        OffsetCommitter committer = new OffsetCommitter(commitQueue, acks, kafkaConsumer, new OffsetState(acknowledgeTimeoutMs));
        committer.push(records);

        Thread blockingThread = new Thread(() -> committer.push(records));
        blockingThread.start();

        Thread.sleep(200);
        closeWorker(committer, 100);
        assertEquals(1, commitQueue.size());
        commitQueue.clear();
    }

    @Test
    public void shouldCommitOffsetsInSequenceWhenAcknowledgedRandom() throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> record1CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record2CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record3CommitOffset = mock(Map.class);
        Records records2 = mock(Records.class);
        Records records3 = mock(Records.class);
        Records records1 = mock(Records.class);
        when(records1.getPartitionsCommitOffset()).thenReturn(record1CommitOffset);
        when(records2.getPartitionsCommitOffset()).thenReturn(record2CommitOffset);
        when(records3.getPartitionsCommitOffset()).thenReturn(record3CommitOffset);
        OffsetCommitter committer = new OffsetCommitter(new LinkedBlockingQueue<>(), acknowledgements, kafkaConsumer, offsetState);

        Arrays.asList(records1, records2, records3).forEach(rs -> committer.push(rs));
        committer.acknowledge(record3CommitOffset);
        committer.acknowledge(record1CommitOffset);
        committer.acknowledge(record2CommitOffset);
        Thread committerThread = new Thread(committer);

        committerThread.start();
        closeWorker(committer, 3000);
        committerThread.join();

        InOrder inOrder = inOrder(kafkaConsumer, offsetState);
        inOrder.verify(offsetState).startTimer();
        inOrder.verify(kafkaConsumer).commitSync(record1CommitOffset);
        inOrder.verify(offsetState, atLeastOnce()).resetOffset(record2CommitOffset);

        inOrder.verify(kafkaConsumer).commitSync(record2CommitOffset);
        inOrder.verify(offsetState, atLeastOnce()).resetOffset(record3CommitOffset);

        inOrder.verify(kafkaConsumer).commitSync(record3CommitOffset);
        inOrder.verify(offsetState, never()).resetOffset(null);
        inOrder.verify(offsetState, never()).shouldCloseConsumer(record3CommitOffset);

        assertTrue(commitQ.isEmpty());
        assertTrue(acknowledgements.isEmpty());
    }

    @Test
    public void shouldCommitInSequenceWithParallelAcknowledgements() throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> record1CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record2CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record3CommitOffset = mock(Map.class);
        Records records2 = mock(Records.class);
        Records records3 = mock(Records.class);
        Records records1 = mock(Records.class);
        int ackDelayRandomMs = 100;
        when(records1.getPartitionsCommitOffset()).thenReturn(record1CommitOffset);
        when(records2.getPartitionsCommitOffset()).thenReturn(record2CommitOffset);
        when(records3.getPartitionsCommitOffset()).thenReturn(record3CommitOffset);
        offsetCommitter.setDefaultSleepMs(50);
        List<Records> incomingRecords = Arrays.asList(records1, records2, records3);
        incomingRecords.forEach(rs -> offsetCommitter.push(rs));

        Thread commiterThread = new Thread(offsetCommitter);
        commiterThread.start();

        Acknowledger acknowledger1 = new Acknowledger(record2CommitOffset, offsetCommitter, new Random().nextInt(ackDelayRandomMs));
        Acknowledger acknowledger2 = new Acknowledger(record1CommitOffset, offsetCommitter, new Random().nextInt(ackDelayRandomMs));
        Acknowledger acknowledger3 = new Acknowledger(record3CommitOffset, offsetCommitter, new Random().nextInt(ackDelayRandomMs));
        List<Acknowledger> acknowledgers = Arrays.asList(acknowledger1, acknowledger2, acknowledger3);
        acknowledgers.forEach(Thread::start);
        closeWorker(offsetCommitter, ackDelayRandomMs + 10);
        commiterThread.join();
        InOrder inOrder = inOrder(kafkaConsumer);
        incomingRecords.forEach(rs -> inOrder.verify(kafkaConsumer).commitSync(rs.getPartitionsCommitOffset()));
        assertTrue(commitQ.isEmpty());
        assertTrue(acknowledgements.isEmpty());
    }

    @Test
    public void shouldStopProcessWhenCommitterGetException() throws InterruptedException {
        doThrow(ConcurrentModificationException.class).when(kafkaConsumer).commitSync(anyMap());
        commitQ.put(records);
        when(records.getPartitionsCommitOffset()).thenReturn(commitPartitionsOffset);
        acknowledgements.add(commitPartitionsOffset);

        Thread commiterThread = new Thread(offsetCommitter);
        commiterThread.start();

        closeWorker(offsetCommitter, 500).join();
        commiterThread.join();

        verify(kafkaConsumer).commitSync(anyMap());
        verify(kafkaConsumer, atLeastOnce()).wakeup(anyString());
    }
}

class Acknowledger extends Thread {

    private Map<TopicPartition, OffsetAndMetadata> offset;
    private Committer committer;
    private long sleepMs;

    Acknowledger(Map<TopicPartition, OffsetAndMetadata> offset, Committer committer, long sleepMs) {
        this.offset = offset;
        this.committer = committer;
        this.sleepMs = sleepMs;
    }

    @Override
    public void run() {
        try {
            sleep(sleepMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        committer.acknowledge(offset);
    }
}
