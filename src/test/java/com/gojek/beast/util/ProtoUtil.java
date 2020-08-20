package com.gojek.beast.util;

import com.gojek.beast.Status;
import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import java.time.Instant;

public class ProtoUtil {
    private static int call = 0;
    private static final int TRIP_DURATION_NANOS = 1000000000;

    public static TestMessage generateTestMessage(Instant now) {
        call++;
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return TestMessage.newBuilder()
                .setOrderNumber("order-" + call)
                .setOrderUrl("order-url-" + call)
                .setOrderDetails("order-details-" + call)
                .setCreatedAt(createdAt)
                .setStatus(Status.COMPLETED)
                .setTripDuration(Duration.newBuilder().setSeconds(1).setNanos(TRIP_DURATION_NANOS).build())
                .addUpdatedAt(createdAt)
                .addUpdatedAt(createdAt)
                .build();

    }

    public static TestNestedMessage generateTestNestedMessage(String nestedId, TestMessage message) {
        return TestNestedMessage.newBuilder()
                .setSingleMessage(message)
                .setNestedId(nestedId)
                .build();
    }
}
