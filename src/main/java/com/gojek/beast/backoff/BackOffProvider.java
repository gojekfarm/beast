package com.gojek.beast.backoff;

public interface BackOffProvider {
    void backOff(int attemptCount);
}
