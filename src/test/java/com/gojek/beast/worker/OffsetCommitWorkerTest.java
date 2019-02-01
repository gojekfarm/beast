package com.gojek.beast.worker;

import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.commiter.OffsetAcknowledger;
import com.gojek.beast.commiter.OffsetState;
import com.gojek.beast.consumer.KafkaConsumer;
import com.gojek.beast.models.Records;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
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
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
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
public class OffsetCommitWorkerTest {

    @Mock
    private Records records;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> commitPartitionsOffset;
    @Mock
    private KafkaConsumer kafkaConsumer;
    private BlockingQueue<Records> commitQ;
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgements;
    private int acknowledgeTimeoutMs;
    private OffsetCommitWorker offsetCommitWorker;
    @Mock
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgeSetMock;
    @Mock
    private OffsetState offsetState;
    private Acknowledger offsetAcknowledger;
    private WorkerState workerState;

    @Before
    public void setUp() {
        commitQ = new LinkedBlockingQueue<>();
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
        acknowledgements = Collections.synchronizedSet(ackSet);
        when(offsetState.shouldCloseConsumer(any())).thenReturn(false);
        workerState = new WorkerState();
        offsetCommitWorker = new OffsetCommitWorker("committer", acknowledgements, kafkaConsumer, offsetState, commitQ, workerState);
        acknowledgeTimeoutMs = 15000;
        offsetAcknowledger = new OffsetAcknowledger(acknowledgements);
    }

    @After
    public void tearDown() throws Exception {
        commitQ.clear();
        acknowledgements.clear();
        offsetCommitWorker.stop("some reason");
    }


    @Test
    public void shouldCommitFirstOffsetWhenAcknowledged() throws InterruptedException {
        when(records.getPartitionsCommitOffset()).thenReturn(commitPartitionsOffset);
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = spy(new CopyOnWriteArraySet<>());
        Set<Map<TopicPartition, OffsetAndMetadata>> acks = Collections.synchronizedSet(ackSet);
        BlockingQueue<Records> commitQueue = spy(new LinkedBlockingQueue<>());
        offsetAcknowledger = new OffsetAcknowledger(ackSet);
        OffsetCommitWorker committer = new OffsetCommitWorker("committer", acks, kafkaConsumer, offsetState, commitQueue, workerState);
        committer.setDefaultSleepMs(10);
        commitQueue.put(records);
        offsetAcknowledger.acknowledge(commitPartitionsOffset);

        Thread commitThread = new Thread(committer);
        commitThread.start();

        await().until(commitQueue::isEmpty);
        workerState.closeWorker();

        verify(commitQueue, atLeast(1)).peek();

        InOrder callOrder = inOrder(kafkaConsumer, records);
        callOrder.verify(records, atLeast(1)).getPartitionsCommitOffset();
        callOrder.verify(kafkaConsumer, times(1)).commitSync(commitPartitionsOffset);
        assertTrue(commitQueue.isEmpty());
        assertTrue(acks.isEmpty());
    }

    @Test
    public void shouldCommitOffsetsInSequenceWhenAcknowledgedRandom() throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> record1CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record2CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record3CommitOffset = mock(Map.class);
        Records records1 = mock(Records.class);
        Records records2 = mock(Records.class);
        Records records3 = mock(Records.class);
        when(records1.getPartitionsCommitOffset()).thenReturn(record1CommitOffset);
        when(records2.getPartitionsCommitOffset()).thenReturn(record2CommitOffset);
        when(records3.getPartitionsCommitOffset()).thenReturn(record3CommitOffset);
        LinkedBlockingQueue<Records> commitQueue = new LinkedBlockingQueue<>();
        OffsetCommitWorker committer = new OffsetCommitWorker("committer", acknowledgements, kafkaConsumer, offsetState, commitQueue, workerState);

        for (Records rs : Arrays.asList(records1, records2, records3)) {
            commitQueue.offer(rs, 1, TimeUnit.SECONDS);
        }
        offsetAcknowledger.acknowledge(record3CommitOffset);
        offsetAcknowledger.acknowledge(record1CommitOffset);
        offsetAcknowledger.acknowledge(record2CommitOffset);
        Thread committerThread = new Thread(committer, "commiter");

        committerThread.start();
        await().until(() -> acknowledgements.isEmpty());
        workerState.closeWorker();
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
        offsetCommitWorker.setDefaultSleepMs(50);
        List<Records> incomingRecords = Arrays.asList(records1, records2, records3);
        for (Records incomingRecord : incomingRecords) {
            commitQ.offer(incomingRecord, 1, TimeUnit.SECONDS);
        }

        Thread commiterThread = new Thread(offsetCommitWorker);
        commiterThread.start();

        AcknowledgeHelper acknowledger1 = new AcknowledgeHelper(record2CommitOffset, offsetAcknowledger, new Random().nextInt(ackDelayRandomMs));
        AcknowledgeHelper acknowledger2 = new AcknowledgeHelper(record1CommitOffset, offsetAcknowledger, new Random().nextInt(ackDelayRandomMs));
        AcknowledgeHelper acknowledger3 = new AcknowledgeHelper(record3CommitOffset, offsetAcknowledger, new Random().nextInt(ackDelayRandomMs));
        List<AcknowledgeHelper> acknowledgers = Arrays.asList(acknowledger1, acknowledger2, acknowledger3);
        acknowledgers.forEach(Thread::start);

        await().until(() -> commitQ.isEmpty());
        workerState.closeWorker();
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

        Thread commiterThread = new Thread(offsetCommitWorker);
        commiterThread.start();

        commiterThread.join();

        verify(kafkaConsumer).commitSync(anyMap());
        verify(kafkaConsumer, atLeastOnce()).wakeup(anyString());
    }
}

class AcknowledgeHelper extends Thread {

    private Map<TopicPartition, OffsetAndMetadata> offset;
    private Acknowledger committer;
    private long sleepMs;

    AcknowledgeHelper(Map<TopicPartition, OffsetAndMetadata> offset, Acknowledger committer, long sleepMs) {
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
