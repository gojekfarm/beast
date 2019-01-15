package com.gojek.beast.backoff;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackOff {

    public void inMilliSeconds(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            log.error("Backoff thread sleep for {} milliseconds interrupted : {} {}",
                    milliseconds, e.getClass(), e.getMessage());
        }
    }
}
