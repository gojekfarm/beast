package com.gojek.beast.worker;

import com.gojek.beast.config.WorkerConfig;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.Record;

import java.util.concurrent.BlockingQueue;

public class BqQueueWorker implements Worker {
    // Should have separate instance of sink for this worker
    private final Sink<Record> sink;
    private final WorkerConfig config;
    private final BlockingQueue<Iterable<Record>> queue;
    private volatile boolean stop;

    public BqQueueWorker(BlockingQueue<Iterable<Record>> queue, Sink<Record> sink, WorkerConfig config) {
        this.queue = queue;
        this.sink = sink;
        this.config = config;
    }

    @Override
    public void run() {
        do {
            try {
                Iterable<Record> poll = queue.poll(config.getTimeout(), config.getTimeoutUnit());
                if (poll != null) {
                    sink.push(poll);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while (!stop);
    }

    @Override
    public void stop() {
        stop = true;
        sink.close();
    }
}
