package com.gojek.beast.worker;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StopEvent {
    private String source;
    private String reason;

    @Override
    public String toString() {
        return "StopEvent{"
                + "reason='" + reason + '\''
                + ", source='" + source + '\''
                + '}';
    }
}
