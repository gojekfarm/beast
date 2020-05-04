package com.gojek.beast.converter.fields;

import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class EnumField implements ProtoField {
    private static final String ENUM = "ENUM";

    @Override
    public Object getValue(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue) {
        if (fieldDescriptor.isRepeated()) {
            List<Descriptors.EnumValueDescriptor> enumValues = ((List<Descriptors.EnumValueDescriptor>) (fieldValue));
            List<String> enumStrValues = new ArrayList<>();
            for (Descriptors.EnumValueDescriptor enumVal : enumValues) {
                enumStrValues.add(enumVal.toString());
            }
            return enumStrValues;
        }
        return fieldValue.toString();
    }

    @Override
    public boolean matches(Descriptors.FieldDescriptor fieldDescriptor, Object fieldValue)  {
        return fieldDescriptor.getJavaType().name().equals(ENUM);
    }
}
