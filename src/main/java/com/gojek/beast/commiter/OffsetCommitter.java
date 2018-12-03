package com.gojek.beast.commiter;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import lombok.AllArgsConstructor;

import java.util.Queue;

@AllArgsConstructor
public class OffsetCommitter implements Sink {

    private Queue<Records> commitQueue;

    public Status push(Records records) {
        commitQueue.add(records);
        return new SuccessStatus();
    }

    @Override
    public void close() {
    }
}
