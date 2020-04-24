package com.gojek.beast.converter;

import com.gojek.beast.converter.fields.ByteField;
import com.gojek.beast.converter.fields.DefaultProtoField;
import com.gojek.beast.converter.fields.EnumField;
import com.gojek.beast.converter.fields.NestedField;
import com.gojek.beast.converter.fields.ProtoField;
import com.gojek.beast.converter.fields.StructField;
import com.gojek.beast.converter.fields.TimestampField;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class FieldFactory {
    private static final List<ProtoField> PROTO_FIELDS = Arrays.asList(
            new TimestampField(),
            new EnumField(),
            new ByteField(),
            new StructField(),
            new NestedField()
    );
    private static final DefaultProtoField DEFAULT_PROTO_FIELD = new DefaultProtoField();

    public static ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {

        Optional<ProtoField> first = PROTO_FIELDS
                .stream()
                .filter(field -> field.matches(descriptor, fieldValue))
                .findFirst();
        return first.orElse(DEFAULT_PROTO_FIELD);
    }

}
