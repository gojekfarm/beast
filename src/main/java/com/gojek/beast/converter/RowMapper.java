package com.gojek.beast.converter;

import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.converter.fields.ProtoField;
import com.gojek.beast.models.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public class RowMapper {

    private final ColumnMapping mapping;

    public Map<String, Object> map(DynamicMessage message) {
        if (mapping == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        Descriptors.Descriptor descriptorForType = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(mapping.size());
        mapping.forEach((key, value) -> {
            String columnName = value.toString();
            Integer protoIndex = Integer.valueOf(key.toString());

            Descriptors.FieldDescriptor fieldDesc = descriptorForType.findFieldByNumber(protoIndex);
            if (fieldDesc != null) {
                Object field = message.getField(fieldDesc);
                ProtoField protoField = FieldFactory.getField(fieldDesc, field);
                Object fieldValue = protoField.getValue();
                row.put(columnName, fieldValue);
            }
        });
        return row;
    }
}
