package com.gojek.beast.converter.fields;


import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.Base64;

@AllArgsConstructor
public class ByteField implements ProtoField {
    private static final String BYTES = "BYTES";

    @Override
    public Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        ByteString byteString = (ByteString) fieldValue;
        return new String(Base64.getEncoder().encode(byteString.toStringUtf8().getBytes()));
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return fieldDescriptor.getType().name().equals(BYTES);
    }
}
