package com.gojek.beast.commiter;

import com.gojek.beast.Clock;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.consumer.KafkaConsumer;
import com.gojek.beast.models.OffsetMap;
import com.gojek.beast.util.RecordsUtil;
import com.gojek.beast.worker.OffsetCommitWorker;
import com.gojek.beast.worker.WorkerState;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;

@RunWith(MockitoJUnitRunner.class)
public class OffsetCommitWorkerIntegrationTest {
    private Set<OffsetMap> acknowledgements;

    @Mock
    private KafkaConsumer kafkaConsumer;
    @Captor
    private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commitPartitionsOffsetCaptor;
    private int acknowledgeTimeoutMs;
    private LinkedBlockingQueue<OffsetMap> commitQueue;
    private RecordsUtil recordsUtil;
    private OffsetCommitWorker committer;
    private OffsetState offsetState;
    private Acknowledger offsetAcknowledger;
    private WorkerState workerState;
    private Clock clock;

    @Before
    public void setUp() {
        commitQueue = new LinkedBlockingQueue<>();
        clock = new Clock();
        CopyOnWriteArraySet<OffsetMap> ackSet = new CopyOnWriteArraySet<>();
        acknowledgements = Collections.synchronizedSet(ackSet);
        acknowledgeTimeoutMs = 2000;
        recordsUtil = new RecordsUtil();
        offsetState = new OffsetState(acknowledgements, acknowledgeTimeoutMs, 1000);
        offsetAcknowledger = new OffsetAcknowledger(acknowledgements);
        workerState = new WorkerState();
        committer = new OffsetCommitWorker("committer", new QueueConfig(200), kafkaConsumer, offsetState, commitQueue, workerState, clock);
    }

    @Test
    public void shouldCommitPartitionsOfAllRecordsInSequence() throws InterruptedException {
        OffsetMap offsetMap1 = recordsUtil.createRecords("driver-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap2 = recordsUtil.createRecords("customer-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap3 = recordsUtil.createRecords("merchant-", 3).getPartitionsCommitOffset();
        List<OffsetMap> recordsList = Arrays.asList(offsetMap1, offsetMap2, offsetMap3);
        commitQueue.addAll(recordsList);
        committer.setDefaultSleepMs(10);

        Thread committerThread = new Thread(committer);
        committerThread.start();

        new Thread(() -> {
            Arrays.asList(2, 1, 0).forEach(index -> {
                OffsetMap partitionsCommitOffset = recordsList.get(index);
                boolean acknowledge = offsetAcknowledger.acknowledge(partitionsCommitOffset);
                assertTrue("Couldn't ack" + partitionsCommitOffset + " " + index, acknowledge);
            });
        }).start();

        await().atMost(30, TimeUnit.SECONDS).until(() -> commitQueue.isEmpty() && acknowledgements.isEmpty());
        workerState.closeWorker();
        committerThread.join();

        InOrder inOrder = inOrder(kafkaConsumer);
        verify(kafkaConsumer).commitSync(commitPartitionsOffsetCaptor.capture());
        List<Map.Entry<TopicPartition, OffsetAndMetadata>> committedOffsets = commitPartitionsOffsetCaptor.getValue().entrySet().stream().sorted(Comparator.comparing(e -> e.getKey().topic())).collect(Collectors.toList());
        assertEquals(3, committedOffsets.size());
        Map.Entry<TopicPartition, OffsetAndMetadata> offset1 = committedOffsets.get(0);
        Map.Entry<TopicPartition, OffsetAndMetadata> offset2 = committedOffsets.get(1);
        Map.Entry<TopicPartition, OffsetAndMetadata> offset3 = committedOffsets.get(2);
        assertEquals("topic_customer-", offset1.getKey().topic());
        assertEquals(0, offset1.getKey().partition());
        assertEquals(6, offset1.getValue().offset());
        assertEquals("topic_driver-", offset2.getKey().topic());
        assertEquals(0, offset2.getKey().partition());
        assertEquals(3, offset2.getValue().offset());
        assertEquals("topic_merchant-", offset3.getKey().topic());
        assertEquals(0, offset3.getKey().partition());
        assertEquals(9, offset3.getValue().offset());
        inOrder.verify(kafkaConsumer, atLeastOnce()).wakeup(anyString());
        assertTrue(acknowledgements.isEmpty());
    }

    @Test
    public void shouldStopConsumerWhenAckTimeOutHappens() throws InterruptedException {
        OffsetMap offsetMap1 = recordsUtil.createRecords("driver-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap2 = recordsUtil.createRecords("customer-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap3 = recordsUtil.createRecords("merchant-", 3).getPartitionsCommitOffset();
        List<OffsetMap> recordsList = Arrays.asList(offsetMap1, offsetMap2, offsetMap3);
        commitQueue.addAll(recordsList);
        committer.setDefaultSleepMs(10);
        List<OffsetMap> ackRecordsList = Arrays.asList(offsetMap1, offsetMap3);
        Thread committerThread = new Thread(committer);
        committerThread.start();

        Thread ackThread = new Thread(() -> ackRecordsList.forEach(partitionsCommitOffset -> {
            try {
                Thread.sleep(new Random().nextInt(10));
            } catch (InterruptedException e) {
            }
            assertTrue("couldn't ack" + partitionsCommitOffset, offsetAcknowledger.acknowledge(partitionsCommitOffset));
        }));

        ackThread.start();
        ackThread.join();
        committerThread.join();

        InOrder inOrder = inOrder(kafkaConsumer);
        inOrder.verify(kafkaConsumer, never()).commitSync(anyMap());
        assertEquals(1, commitQueue.size());
        assertEquals(offsetMap3, commitQueue.take());
        inOrder.verify(kafkaConsumer, atLeastOnce()).wakeup(anyString());
        assertEquals(1, acknowledgements.size());
        assertEquals(offsetMap3, acknowledgements.stream().findFirst().get());
    }

    @Test
    public void shouldStopWhenNoAcknowledgements() throws InterruptedException {
        OffsetMap offsetMap1 = recordsUtil.createRecords("driver-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap2 = recordsUtil.createRecords("customer-", 3).getPartitionsCommitOffset();
        OffsetMap offsetMap3 = recordsUtil.createRecords("merchant-", 3).getPartitionsCommitOffset();
        List<OffsetMap> recordsList = Arrays.asList(offsetMap1, offsetMap2, offsetMap3);
        commitQueue.addAll(recordsList);
        committer.setDefaultSleepMs(10);
        Thread committerThread = new Thread(committer);
        committerThread.start();

        committerThread.join();

        InOrder inOrder = inOrder(kafkaConsumer);
        inOrder.verify(kafkaConsumer, never()).commitSync(anyMap());
        assertEquals(2, commitQueue.size());
        inOrder.verify(kafkaConsumer, atLeastOnce()).wakeup(anyString());
        assertTrue(acknowledgements.isEmpty());
    }
}
