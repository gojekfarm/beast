package com.gojek.beast.worker;

import com.gojek.beast.models.Status;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Worker extends Thread {
    private static StopEvent stopEvent;
    private final WorkerState state;

    public Worker(String name, WorkerState state) {
        super(name);
        this.state = state;
    }

    public abstract void stop(String reason);

    protected abstract Status job();

    @Override
    public void run() {
        log.info("Started worker {}", getClass().getSimpleName());
        Status status;
        do {
            status = job();
        } while (!state.isStopped() && status.isSuccess());
        onStopEvent(status.toString());
    }

    private void onStopEvent(String reason) {
        log.debug("{} returned Error::{}, stopping other worker threads", getClass().getSimpleName(), reason);
        if (stopEvent == null) {
            stopEvent = new StopEvent(getClass().getSimpleName(), reason);
        }
        state.closeWorker();
        stop(stopEvent.toString());
        log.info("Stopped worker {} job status: {}, reason: {}", getClass().getSimpleName(), reason, stopEvent);
    }
}
