package com.gojek.beast.worker;

import com.gojek.beast.config.WorkerConfig;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BqQueueWorkerTest {
    @Mock
    private Sink<Record> bqSink;
    @Mock
    private Iterable<Record> messages;
    private WorkerConfig workerConfig;
    private int pollTimeout;

    @Before
    public void setUp() {
        pollTimeout = 200;
        workerConfig = new WorkerConfig(pollTimeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void shouldReadFromQueueAndPushToSink() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, bqSink, workerConfig);
        queue.put(messages);

        Thread thread = new Thread(worker);
        thread.start();

        closeWorker(worker, 100);
        thread.join();
        verify(bqSink).push(messages);
    }

    @Test
    public void shouldReadFromQueueForeverAndPushToSink() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, bqSink, workerConfig);
        Iterable<Record> messages2 = mock(Iterable.class);
        queue.put(messages);
        queue.put(messages2);

        Thread workerThread = new Thread(worker);
        workerThread.start();

        closeWorker(worker, 100);
        workerThread.join();
        verify(bqSink).push(messages);
        verify(bqSink).push(messages2);
    }

    @Test
    public void shouldNotPushToSinkIfNoMessage() throws InterruptedException {
        BlockingQueue<Iterable<Record>> queue = new LinkedBlockingQueue<>();
        BqQueueWorker worker = new BqQueueWorker(queue, bqSink, workerConfig);
        Thread workerThread = new Thread(worker);

        workerThread.start();

        closeWorker(worker, 200);
        workerThread.join();
        verify(bqSink, never()).push(any());
    }

    private void closeWorker(BqQueueWorker worker, int sleepMillis) {
        new Thread(() -> {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            worker.stop();
        }).start();
    }
}
