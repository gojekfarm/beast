package com.gojek.beast.converter.fields;

import com.google.cloud.Date;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class DateField implements ProtoField {
    private final Descriptors.FieldDescriptor descriptor;
    private final Object fieldValue;

    @Override
    public Object getValue() {
        DynamicMessage dynamicField = (DynamicMessage) fieldValue;
        List<Descriptors.FieldDescriptor> descriptors = dynamicField.getDescriptorForType().getFields();
        List<Object> dateFields = new ArrayList<>();
        descriptors.forEach(desc -> dateFields.add(dynamicField.getField(desc)));
        Integer year = (Integer) dateFields.get(0);
        Integer month = (Integer) dateFields.get(1);
        Integer day = (Integer) dateFields.get(2);
        return Date.fromYearMonthDay(year, month, day);
    }

    @Override
    public boolean matches() {
        return descriptor.getJavaType().name().equals("MESSAGE")
                && descriptor.getMessageType().getFullName().equals(com.google.type.Date.getDescriptor().getFullName());
    }
}
