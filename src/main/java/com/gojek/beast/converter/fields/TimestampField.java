package com.gojek.beast.converter.fields;

import com.google.api.client.util.DateTime;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class TimestampField implements ProtoField {
    private static final String MESSAGE = "MESSAGE";

    @Override
    public Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        DynamicMessage dynamicField = (DynamicMessage) fieldValue;
        List<Descriptors.FieldDescriptor> descriptors = dynamicField.getDescriptorForType().getFields();
        List<Object> timeFields = new ArrayList<>();
        descriptors.forEach(desc -> timeFields.add(dynamicField.getField(desc)));
        Instant time = Instant.ofEpochSecond((long) timeFields.get(0), ((Integer) timeFields.get(1)).longValue());
        return new DateTime(time.toEpochMilli());
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return fieldDescriptor.getJavaType().name().equals(MESSAGE)
                && fieldDescriptor.getMessageType().getFullName().equals(com.google.protobuf.Timestamp.getDescriptor().getFullName());
    }
}
