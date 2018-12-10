package com.gojek.beast.commiter;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.util.WorkerUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
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
    private Queue<Records> commitQ;
    private Set<Map<TopicPartition, OffsetAndMetadata>> acknowledgements;
    private OffsetCommitter offsetCommitter = new OffsetCommitter(commitQ, acknowledgements, kafkaConsumer);

    @Before
    public void setUp() {
        commitQ = new LinkedBlockingQueue<>();
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
        acknowledgements = Collections.synchronizedSet(ackSet);
        offsetCommitter = new OffsetCommitter(commitQ, acknowledgements, kafkaConsumer);
    }

    @After
    public void tearDown() throws Exception {
        commitQ.clear();
        acknowledgements.clear();
        offsetCommitter.stop();
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

    @Test
    public void shouldCommitFirstOffsetWhenAcknowledged() {
        when(records.getPartitionsCommitOffset()).thenReturn(commitPartitionsOffset);
        CopyOnWriteArraySet<Map<TopicPartition, OffsetAndMetadata>> ackSet = new CopyOnWriteArraySet<>();
        Set<Map<TopicPartition, OffsetAndMetadata>> acks = Collections.synchronizedSet(ackSet);
        OffsetCommitter committer = new OffsetCommitter(new LinkedBlockingQueue<>(), acks, kafkaConsumer);
        committer.push(records);
        committer.acknowledge(commitPartitionsOffset);

        new Thread(committer).start();

        WorkerUtil.closeWorker(committer, 500);
        verify(kafkaConsumer).commitSync(commitPartitionsOffset);
        assertTrue(commitQ.isEmpty());
        assertTrue(acks.isEmpty());
    }

    @Test
    public void shouldCommitOffsetsInSequenceWhenAcknowledgedRandom() {
        Map<TopicPartition, OffsetAndMetadata> record1CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record2CommitOffset = mock(Map.class);
        Map<TopicPartition, OffsetAndMetadata> record3CommitOffset = mock(Map.class);
        Records records2 = mock(Records.class);
        Records records3 = mock(Records.class);
        Records records1 = mock(Records.class);
        when(records1.getPartitionsCommitOffset()).thenReturn(record1CommitOffset);
        when(records2.getPartitionsCommitOffset()).thenReturn(record2CommitOffset);
        when(records3.getPartitionsCommitOffset()).thenReturn(record3CommitOffset);
        OffsetCommitter committer = new OffsetCommitter(new LinkedBlockingQueue<>(), acknowledgements, kafkaConsumer);

        Arrays.asList(records1, records2, records3).forEach(rs -> committer.push(rs));
        committer.acknowledge(record3CommitOffset);
        committer.acknowledge(record1CommitOffset);
        committer.acknowledge(record2CommitOffset);

        new Thread(committer).start();

        InOrder inOrder = inOrder(kafkaConsumer);
        WorkerUtil.closeWorker(committer, 1000);

        inOrder.verify(kafkaConsumer).commitSync(record1CommitOffset);
        inOrder.verify(kafkaConsumer).commitSync(record2CommitOffset);
        inOrder.verify(kafkaConsumer).commitSync(record3CommitOffset);

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
        WorkerUtil.closeWorker(offsetCommitter, ackDelayRandomMs + 10);
        commiterThread.join();
        InOrder inOrder = inOrder(kafkaConsumer);
        incomingRecords.forEach(rs -> inOrder.verify(kafkaConsumer).commitSync(rs.getPartitionsCommitOffset()));
        assertTrue(commitQ.isEmpty());
        assertTrue(acknowledgements.isEmpty());
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
