package com.gojek.beast.worker;

import com.gojek.beast.models.Status;
import com.gojek.beast.stats.Stats;
import lombok.extern.slf4j.Slf4j;
import org.greenrobot.eventbus.EventBus;
import org.greenrobot.eventbus.Subscribe;
import org.greenrobot.eventbus.ThreadMode;

@Slf4j
public abstract class CoolWorker implements Worker {

    private boolean stopWorker;

    public abstract void stop(String reason);

    abstract Status job();

    private final Stats statsClient = Stats.client();

    @Override
    public void run() {
        EventBus.getDefault().register(this);
        Status status;
        do {
            status = job();
        } while (!stopWorker && status.isSuccess());

        if (!stopWorker) {
            EventBus.getDefault().post(new StopEvent(status.toString()));
        }
        EventBus.getDefault().unregister(this);
    }

    @Subscribe(threadMode = ThreadMode.MAIN_ORDERED)
    public void onStopEvent(StopEvent event) {
        log.debug("Stopping worker {} on receiving event {}", getClass().getSimpleName(), event);
        stop(event.getReason());
        stopWorker = true;
    }
}

