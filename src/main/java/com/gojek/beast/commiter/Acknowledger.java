package com.gojek.beast.commiter;

import com.gojek.beast.models.OffsetMap;

public interface Acknowledger {
    boolean acknowledge(OffsetMap offsets);

    void close(String reason);
}
