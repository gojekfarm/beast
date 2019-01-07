package com.gojek.beast.worker;

import com.gojek.beast.commiter.Committer;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.util.RecordsUtil;
import com.gojek.beast.util.WorkerUtil;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
    private Committer committer;
    @Mock
    private Sink failureSink;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> offsetInfos;
    private int maxPushAttempts;
    private RecordsUtil recordUtil;

    @Before
    public void setUp() {
        pollTimeout = 200;
        maxPushAttempts = 1;
        recordUtil = new RecordsUtil();
        queueConfig = new QueueConfig(pollTimeout, maxPushAttempts);
        when(successfulSink.push(any())).thenReturn(new SuccessStatus());
        when(messages.getPartitionsCommitOffset()).thenReturn(offsetInfos);
        when(committer.acknowledge(any())).thenReturn(true);
    }

    @Test
    public void shouldReadFromQueueAndPushToSink() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, queueConfig, committer);
        queue.put(messages);

        Thread thread = new Thread(worker);
        thread.start();

        WorkerUtil.closeWorker(worker, 100);
        thread.join();
        verify(successfulSink).push(messages);
    }

    @Test
    public void shouldReadFromQueueForeverAndPushToSink() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, queueConfig, committer);
        Records messages2 = mock(Records.class);
        when(committer.acknowledge(any())).thenReturn(true);
        queue.put(messages);
        queue.put(messages2);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        Thread closer = WorkerUtil.closeWorker(worker, 1000);
        workerThread.join();
        closer.join();
        verify(successfulSink).push(messages);
        verify(successfulSink).push(messages2);
    }

    @Test
    public void shouldAckAfterSuccessfulPush() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, queueConfig, committer);
        queue.put(messages);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, 200);
        workerThread.join();
        verify(successfulSink).push(messages);
        verify(committer).acknowledge(offsetInfos);
    }

    @Test
    public void shouldNotAckAfterFailurePush() throws InterruptedException {
        when(failureSink.push(messages)).thenReturn(new FailureStatus(new Exception()));
        when(messages.getPushAttempts()).thenReturn(1, 2, 3, 4, 5);
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, failureSink, queueConfig, committer);

        queue.put(messages);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, 100);
        workerThread.join();
        verify(failureSink).push(messages);
        verify(committer, never()).acknowledge(any());
    }

    @Test
    public void shouldNotPushToSinkIfNoMessage() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, queueConfig, committer);
        Thread workerThread = new Thread(worker);

        workerThread.start();

        WorkerUtil.closeWorker(worker, 200);
        workerThread.join();
        verify(successfulSink, never()).push(any());
    }

    @Test
    public void shouldCloseCommitterWhenBiqQueryExceptionHappens() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        queue.put(messages);
        doThrow(new BigQueryException(10, "Some Error")).when(failureSink).push(messages);
        BqQueueWorker worker = new BqQueueWorker(queue, failureSink, queueConfig, committer);
        Thread workerThread = new Thread(worker);

        workerThread.start();

        WorkerUtil.closeWorker(worker, 1000);
        workerThread.join();
        verify(committer).close();
    }

    @Test
    public void shouldPushToBQSinkUntilMaxAttempts() throws InterruptedException {
        Records poll = recordUtil.createRecords("customer-", 1);
        when(failureSink.push(poll)).thenReturn(new FailureStatus(new Exception()));

        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, failureSink, queueConfig, committer);

        queue.put(poll);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, 100).join();
        workerThread.join();
        verify(failureSink, times(maxPushAttempts)).push(poll);
    }
}
