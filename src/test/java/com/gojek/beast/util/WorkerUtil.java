package com.gojek.beast.util;

import com.gojek.beast.worker.Worker;

public class WorkerUtil {
    public static void closeWorker(Worker worker, int sleepMillis) {
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
