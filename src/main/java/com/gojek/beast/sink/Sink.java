package com.gojek.beast.sink;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;

public interface Sink {
    Status push(Records records);

    void close();
}
