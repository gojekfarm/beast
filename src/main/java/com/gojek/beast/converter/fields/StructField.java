package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StructField implements ProtoField {
    private static final String MESSAGE = "MESSAGE";

    @Override
    public Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        try {
            return JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print((DynamicMessage) fieldValue);
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        return fieldDescriptor.getJavaType().name().equals(MESSAGE)
                && fieldDescriptor.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName());
    }
}
