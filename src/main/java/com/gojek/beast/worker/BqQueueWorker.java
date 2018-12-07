package com.gojek.beast.worker;

import com.gojek.beast.commiter.Committer;
import com.gojek.beast.config.WorkerConfig;
import com.gojek.beast.models.Records;
import com.gojek.beast.sink.Sink;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;

@Slf4j
public class BqQueueWorker implements Worker {
    // Should have separate instance of sink for this worker
    private final Sink sink;
    private final WorkerConfig config;
    private final BlockingQueue<Records> queue;
    private final Committer committer;
    private volatile boolean stop;

    public BqQueueWorker(BlockingQueue<Records> queue, Sink sink, WorkerConfig config, Committer committer) {
        this.queue = queue;
        this.sink = sink;
        this.config = config;
        this.committer = committer;
    }

    @Override
    public void run() {
        do {
            try {
                Records poll = queue.poll(config.getTimeout(), config.getTimeoutUnit());
                if (poll != null && sink.push(poll).isSuccess()) {
                    committer.acknowledge(poll.getPartitionsCommitOffset());
                }
            } catch (InterruptedException e) {
                log.error("Failed to poll records from read queue: ", e);
            }
        } while (!stop);
    }

    @Override
    public void stop() {
        stop = true;
        sink.close();
    }
}
