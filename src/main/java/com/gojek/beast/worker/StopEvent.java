package com.gojek.beast.worker;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class StopEvent {
    private String reason;
}
