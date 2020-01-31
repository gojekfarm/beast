package com.gojek.beast.util;

import com.gojek.beast.worker.Worker;
import com.gojek.beast.worker.WorkerState;

import java.util.List;

public class WorkerUtil {
    public static Thread closeWorkers(List<Worker> workers, List<WorkerState> workerStates, long sleepMillis) {
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(sleepMillis);
                workers.forEach(worker -> worker.stop("some reason"));
                workerStates.forEach(workerState -> workerState.closeWorker());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        closer.start();
        return closer;
    }

    public static Thread closeWorker(Worker worker, WorkerState workerState, long sleepMillis) {
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(sleepMillis);
                workerState.closeWorker();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Closing given Thread with worker util " + sleepMillis);
            worker.stop("some reason");
        });
        closer.start();
        return closer;
    }
}
