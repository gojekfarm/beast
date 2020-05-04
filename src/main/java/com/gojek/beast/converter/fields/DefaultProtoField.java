package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DefaultProtoField implements ProtoField {

    // handles primitives, repeated field
    @Override
    public Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return fieldValue;
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return false;
    }
}
