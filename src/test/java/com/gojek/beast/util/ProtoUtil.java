package com.gojek.beast.util;

import com.gojek.beast.Status;
import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.google.protobuf.Timestamp;

import java.time.Instant;

public class ProtoUtil {
    private static int call = 0;

    public static TestMessage generateTestMessage(Instant now) {
        call++;
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        return TestMessage.newBuilder()
                .setOrderNumber("order-" + call)
                .setOrderUrl("order-url-" + call)
                .setOrderDetails("order-details-" + call)
                .setCreatedAt(createdAt)
                .setStatus(Status.COMPLETED)
                .build();

    }

    public static TestNestedMessage generateTestNestedMessage(String nestedId, TestMessage message) {
        return TestNestedMessage.newBuilder()
                .setSingleMessage(message)
                .setNestedId(nestedId)
                .build();
    }
}
