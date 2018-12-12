package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EnumField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        return fieldValue.toString();
    }

    @Override
    public boolean matches() {
        return descriptor.getJavaType().name().equals("ENUM");
    }
}
