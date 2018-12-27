package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StructField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        try {
            return JsonFormat.printer().print((DynamicMessage) fieldValue).trim().replaceAll("\n", "").replaceAll(" ", "");
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

    @Override
    public boolean matches() {
        return descriptor.getJavaType().name().equals("MESSAGE")
                && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName());
    }
}
