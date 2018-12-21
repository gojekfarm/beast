package com.gojek.beast.converter;

import com.gojek.beast.Status;
import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.gojek.beast.converter.fields.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class FieldFactoryTest {

    private TestMessage message;
    private Timestamp createdAt;

    @Before
    public void setUp() throws Exception {
        Instant now = Instant.now();
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        message = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(Status.COMPLETED)
                .putCurrentState("payment","cash")
                .setUserToken(ByteString.copyFrom("token".getBytes()))
                .setTripDuration(Duration.newBuilder().setSeconds(1).setNanos(1000000000).build())
                .build();
    }

    @Test
    public void shouldReturnTimestampField() {
        Descriptors.FieldDescriptor timestampDesc = message.getDescriptorForType().findFieldByNumber(4);

        ProtoField protoField = FieldFactory.getField(timestampDesc, message.getField(timestampDesc));

        assertEquals(TimestampField.class.getName(), protoField.getClass().getName());
    }

    @Test
    public void shouldReturnEnumField() {
        Descriptors.FieldDescriptor enumDesc = message.getDescriptorForType().findFieldByNumber(5);

        ProtoField protoField = FieldFactory.getField(enumDesc, message.getField(enumDesc));

        assertEquals(EnumField.class.getName(), protoField.getClass().getName());
    }

    @Test
    public void shouldReturnByteField() {
        Descriptors.FieldDescriptor byteDesc = message.getDescriptorForType().findFieldByNumber(10);

        ProtoField protoField = FieldFactory.getField(byteDesc, message.getField(byteDesc));

        assertEquals(ByteField.class.getName(), protoField.getClass().getName());
        String encodedToken = new String(Base64.getEncoder().encode("token".getBytes()));
        assertEquals(encodedToken, protoField.getValue());
    }

    @Test
    public void shouldReturnNestedProtoField() {
        TestNestedMessage nestedMessage = TestNestedMessage.newBuilder()
                .setNestedId("shouldParseNestedMessageSuccessfully-id")
                .setSingleMessage(message)
                .build();
        Descriptors.FieldDescriptor nestedMessageDesc = nestedMessage.getDescriptorForType().findFieldByNumber(2);

        ProtoField protoField = FieldFactory.getField(nestedMessageDesc, nestedMessage.getField(nestedMessageDesc));

        assertEquals(NestedField.class.getName(), protoField.getClass().getName());
    }

    @Test
    public void shouldReturnDurationFieldAsNested() {
        Descriptors.FieldDescriptor durationDesc = message.getDescriptorForType().findFieldByNumber(11);

        ProtoField protoField = FieldFactory.getField(durationDesc, message.getField(durationDesc));

        assertEquals(NestedField.class.getName(), protoField.getClass().getName());
    }
}
