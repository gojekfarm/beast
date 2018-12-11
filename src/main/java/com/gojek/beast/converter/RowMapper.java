package com.gojek.beast.converter;

import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.converter.fields.DefaultProtoField;
import com.gojek.beast.converter.fields.ProtoField;
import com.gojek.beast.converter.fields.TimestampField;
import com.gojek.beast.models.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
public class RowMapper {

    private final ColumnMapping mapping;

    public Map<String, Object> map(DynamicMessage message) {
        if (mapping == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        Map<Descriptors.FieldDescriptor, Object> messageFields = message.getAllFields();
        Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(mapping.size());
        mapping.forEach((key, value) -> {
            String columnName = value.toString();
            Integer protoIndex = Integer.valueOf(key.toString());

            Descriptors.FieldDescriptor fieldDesc = messageDescriptor.findFieldByNumber(protoIndex);
            if (fieldDesc != null) {
                Object field = messageFields.get(fieldDesc);
                Object fieldValue = getField(fieldDesc, field).getValue();
                row.put(columnName, fieldValue);
            }
        });
        return row;
    }

    private ProtoField getField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        List<ProtoField> protoFields = Arrays.asList(new TimestampField(descriptor, fieldValue));
        Optional<ProtoField> first = protoFields
                .stream()
                .filter(ProtoField::matches)
                .findFirst();
        return first.orElseGet(() -> new DefaultProtoField(descriptor, fieldValue));
    }

}
