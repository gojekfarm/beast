package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class NestedField implements ProtoField {
    private static final String MESSAGE = "MESSAGE";

    @Override
    public DynamicMessage getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return (DynamicMessage) fieldValue;
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return fieldDescriptor.getJavaType().name().equals(MESSAGE)
                && !(fieldValue instanceof List);
    }
}
