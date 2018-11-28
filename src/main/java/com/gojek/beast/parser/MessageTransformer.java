package com.gojek.beast.parser;

import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public class MessageTransformer {

    private final ColumnMapping mapping;

    public Map<String, Object> getFields(DynamicMessage message) {
        if (mapping == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        Map<String, Object> columns = new HashMap<>();
        Map<Descriptors.FieldDescriptor, Object> messageFields = message.getAllFields();
        Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();

        mapping.forEach((key, value) -> {
            String columnName = value.toString();
            Integer protoIndex = Integer.valueOf(key.toString());

            Descriptors.FieldDescriptor field = messageDescriptor.findFieldByNumber(protoIndex);
            if (field != null) {
                columns.put(columnName, messageFields.get(field));
            }
        });
        return columns;
    }

}
