package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;

public interface ProtoField {

    Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue);

    boolean matches(Descriptors.FieldDescriptor descriptor, Object fieldValue);
}
