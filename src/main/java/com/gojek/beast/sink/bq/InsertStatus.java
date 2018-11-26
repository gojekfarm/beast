package com.gojek.beast.sink.bq;

import com.gojek.beast.sink.Status;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class InsertStatus implements Status {
    private boolean success;
}
