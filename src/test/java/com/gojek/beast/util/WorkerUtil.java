package com.gojek.beast.util;

import com.gojek.beast.worker.Worker;

public class WorkerUtil {
    public static Thread closeWorker(Worker worker, long sleepMillis) {
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            worker.stop();
        });
        closer.start();
        return closer;
    }
}
