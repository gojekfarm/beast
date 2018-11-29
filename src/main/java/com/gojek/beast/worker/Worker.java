package com.gojek.beast.worker;

public interface Worker extends Runnable {
    void stop();
}
