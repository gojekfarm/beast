package com.gojek.beast.worker;

import com.gojek.beast.commiter.Acknowledger;
import com.gojek.beast.config.QueueConfig;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.util.WorkerUtil;
import com.google.cloud.bigquery.BigQueryException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BqQueueWorkerIntegrationTest {

    private List<Worker> workers = new ArrayList<>();
    private BlockingQueue<Records> queue;
    @Mock
    private Sink sink;
    @Mock
    private Acknowledger committer;
    @Mock
    private Records messages;
    private FailureStatus failureStatus;

    @Before
    public void setUp() throws Exception {
        queue = new LinkedBlockingQueue<>();
        QueueConfig config = new QueueConfig(1000);
        int totalWorkers = 5;

        for (int i = 0; i < totalWorkers; i++) {
            workers.add(new BqQueueWorker(queue, sink, config, committer));
        }
        failureStatus = new FailureStatus(new RuntimeException("BQ Push Failure"));
    }

    @Test
    public void shouldKeepElementInQueueOnSinkPushFailure() throws InterruptedException {
        queue.put(messages);
        when(sink.push(messages)).thenReturn(failureStatus);
        startWorkers(workers);

        Thread closer = WorkerUtil.closeWorkers(workers, 500);
        closer.join();
        assertEquals(1, queue.size());
    }

    @Test
    public void shouldKeepElementInQueueOnException() throws InterruptedException {
        queue.put(messages);
        doThrow(new BigQueryException(10, "failed to push to BQ")).when(sink).push(messages);
        startWorkers(workers);

        Thread closer = WorkerUtil.closeWorkers(workers, 500);
        closer.join();
        assertEquals(1, queue.size());
    }

    private void startWorkers(List<Worker> workerList) {
        workerList.forEach(w -> new Thread(w).start());
    }

    @After
    public void tearDown() throws Exception {
        workers.forEach(worker -> worker.stop("some reason"));
        workers.clear();
        queue.clear();
    }
}
