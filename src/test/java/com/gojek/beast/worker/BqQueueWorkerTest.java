package com.gojek.beast.worker;

import com.gojek.beast.commiter.Committer;
import com.gojek.beast.config.WorkerConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.util.WorkerUtil;
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
    private WorkerConfig workerConfig;
    private int pollTimeout;
    @Mock
    private Committer committer;
    @Mock
    private Sink failureSink;
    @Mock
    private Map<TopicPartition, OffsetAndMetadata> offsetInfos;

    @Before
    public void setUp() {
        pollTimeout = 200;
        workerConfig = new WorkerConfig(pollTimeout);
        when(successfulSink.push(any())).thenReturn(new SuccessStatus());
        when(messages.getPartitionsCommitOffset()).thenReturn(offsetInfos);
    }

    @Test
    public void shouldReadFromQueueAndPushToSink() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, workerConfig, committer);
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
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, workerConfig, committer);
        Records messages2 = mock(Records.class);
        queue.put(messages);
        queue.put(messages2);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, 100);
        workerThread.join();
        verify(successfulSink).push(messages);
        verify(successfulSink).push(messages2);
    }

    @Test
    public void shouldAckAfterSuccessfulPush() throws InterruptedException {
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, workerConfig, committer);
        queue.put(messages);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        WorkerUtil.closeWorker(worker, 1000);
        workerThread.join();
        verify(successfulSink).push(messages);
        verify(committer).acknowledge(offsetInfos);
    }

    @Test
    public void shouldNotAckAfterFailurePush() throws InterruptedException {
        when(failureSink.push(messages)).thenReturn(new FailureStatus(new Exception()));
        BlockingQueue<Records> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, failureSink, workerConfig, committer);

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
        BqQueueWorker worker = new BqQueueWorker(queue, successfulSink, workerConfig, committer);
        Thread workerThread = new Thread(worker);

        workerThread.start();

        WorkerUtil.closeWorker(worker, 200);
        workerThread.join();
        verify(successfulSink, never()).push(any());
    }

}
