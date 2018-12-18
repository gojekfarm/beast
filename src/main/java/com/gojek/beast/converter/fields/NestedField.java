package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NestedField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public DynamicMessage getValue() {
        return (DynamicMessage) fieldValue;
    }

    @Override
    public boolean matches() {
        return descriptor.getJavaType().name().equals("MESSAGE");
    }
}
