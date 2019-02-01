package com.gojek.beast.worker;

public final class WorkerState {

    private boolean stopped;

    boolean isStopped() {
        return stopped;
    }

    public void closeWorker() {
        stopped = true;
    }
}
