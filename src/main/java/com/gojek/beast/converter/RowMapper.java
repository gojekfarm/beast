package com.gojek.beast.converter;

import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.Constants.Config;
import com.gojek.beast.converter.fields.NestedField;
import com.gojek.beast.converter.fields.ProtoField;
import com.gojek.beast.models.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AllArgsConstructor
public class RowMapper {

    private final ColumnMapping mapping;

    public Map<String, Object> map(DynamicMessage message) {
        if (mapping == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        return getMappings(message, mapping);
    }

    private Map<String, Object> getMappings(DynamicMessage message, ColumnMapping columnMapping) {
        if (message == null || columnMapping == null || columnMapping.isEmpty()) {
            return Collections.emptyMap();
        }
        Descriptors.Descriptor descriptorForType = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(columnMapping.size());
        columnMapping.forEach((key, value) -> {
            String columnName = value.toString();
            String column = key.toString();
            if (column.equals(Config.RECORD_NAME)) {
                return;
            }
            Integer protoIndex = Integer.valueOf(column);
            Descriptors.FieldDescriptor fieldDesc = descriptorForType.findFieldByNumber(protoIndex);
            if (fieldDesc != null && !message.getField(fieldDesc).toString().isEmpty()) {
                Object field = message.getField(fieldDesc);
                ProtoField protoField = FieldFactory.getField(fieldDesc, field);
                Object fieldValue = protoField.getValue();

                if (fieldValue instanceof List) {
                    addRepeatedFields(row, (String) key, value, (List<Object>) fieldValue);
                    return;
                }

                if (protoField.getClass().getName().equals(NestedField.class.getName())) {
                    try {
                        columnName = getNestedColumnName((ColumnMapping) value);
                        fieldValue = getMappings((DynamicMessage) field, (ColumnMapping) value);
                    } catch (Exception e) {
                        log.error("Exception::Handling nested field failure: {}", e.getMessage());
                    }
                }
                row.put(columnName, fieldValue);
            }
        });
        return row;
    }

    private String getNestedColumnName(ColumnMapping value) {
        return value.get(Config.RECORD_NAME).toString();
    }

    private void addRepeatedFields(Map<String, Object> row, String key, Object value, List<Object> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        String columnName = null;
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                ColumnMapping nestedMappings = (ColumnMapping) value;
                repeatedNestedFields.add(getMappings((DynamicMessage) f, nestedMappings));
                columnName = getNestedColumnName(nestedMappings);
            } else {
                repeatedNestedFields.add(f);
                columnName = (String) value;
            }
        }
        row.put(columnName, repeatedNestedFields);
    }
}
