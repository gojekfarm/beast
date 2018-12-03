package com.gojek.beast.commiter;

import com.gojek.beast.models.OffsetInfo;

public interface Committer {
    void acknowledge(OffsetInfo offset);
}
