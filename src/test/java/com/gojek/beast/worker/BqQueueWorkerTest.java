package com.gojek.beast.worker;

import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.util.WorkerUtil;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqQueueWorkerTest {

    @Mock
    private Sink successfulSink;
    @Mock
    private Records messages;
    private QueueConfig queueConfig;
    private int pollTimeout;
    @Mock
    private Acknowledger committer;
    @Mock
    private Sink failureSink;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> offsetInfos;
    private WorkerState workerState;

    @Before
    public void setUp() {
        pollTimeout = 200;
        queueConfig = new QueueConfig(pollTimeout);
        when(successfulSink.push(any())).thenReturn(new SuccessStatus());
        when(messages.getPartitionsCommitOffset()).thenReturn(offsetInfos);
        when(messages.getPolledTime()).thenReturn(Instant.now());
        workerState = new WorkerState();
    }

    @Test
    public void shouldReadFromQueueAndPushToSink() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker("bq-worker", successfulSink, queueConfig, committer, queue, workerState);
        queue.put(messages);

        Thread thread = new Thread(worker);
        thread.start();

        WorkerUtil.closeWorker(worker, workerState, 100);
        thread.join();
        verify(successfulSink).push(messages);
    }

    @Test
    public void shouldReadFromQueueForeverAndPushToSink() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker("bq-worker", successfulSink, queueConfig, committer, queue, workerState);
        Records messages2 = mock(Records.class);
        when(committer.acknowledge(any())).thenReturn(true);
        queue.put(messages);
        queue.put(messages2);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> queue.isEmpty());
        workerState.closeWorker();
        workerThread.join();
        verify(successfulSink).push(messages);
        verify(successfulSink).push(messages2);
    }

    @Test
    public void shouldAckAfterSuccessfulPush() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker("bq-worker", successfulSink, queueConfig, committer, queue, workerState);
        queue.put(messages);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, workerState, 200);
        workerThread.join();
        verify(successfulSink).push(messages);
        verify(committer).acknowledge(offsetInfos);
    }

    @Test
    public void shouldNotAckAfterFailurePush() throws InterruptedException {
        when(failureSink.push(messages)).thenReturn(new FailureStatus(new Exception()));
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker("bq-worker", failureSink, queueConfig, committer, queue, workerState);

        queue.put(messages);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, workerState, 100);
        workerThread.join();
        verify(failureSink).push(messages);
        verify(committer, never()).acknowledge(any());
    }

    @Test
    public void shouldNotPushToSinkIfNoMessage() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker("bq-worker", successfulSink, queueConfig, committer, queue, workerState);
        Thread workerThread = new Thread(worker);

        workerThread.start();
        Thread.sleep(100);
        workerState.closeWorker();

        workerThread.join();
        verify(successfulSink, never()).push(any());
    }

    @Test
    public void shouldCloseCommitterWhenBiqQueryExceptionHappens() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queue.put(messages);
        doThrow(new BigQueryException(10, "Some Error")).when(failureSink).push(messages);
        BqQueueWorker worker = new BqQueueWorker("bq-worker", failureSink, queueConfig, committer, queue, workerState);
        Thread workerThread = new Thread(worker);

        workerThread.start();
        workerThread.join();
        //TODO: change Worker run to callable and verify return value
    }
}
