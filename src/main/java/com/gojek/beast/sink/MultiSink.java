package com.gojek.beast.sink;

import com.gojek.beast.models.MultiException;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class MultiSink implements Sink {
    private final List<Sink> sinks;

    @Override
    public Status push(Records records) {
        List<Status> failures = sinks.stream()
                .map(s -> s.push(records))
                .filter(s -> !s.isSuccess())
                .collect(Collectors.toList());
        return failures.isEmpty() ? new SuccessStatus() : new MultiException(failures);
    }

    @Override
    public void close() {
        sinks.forEach(Sink::close);
        log.info("Stopped MultiSink Successfully");
    }
}
