package com.gojek.beast;

import java.time.Instant;

public class Clock {

    public long currentEpochMillis() {
        return Instant.now().toEpochMilli();
    }
}
