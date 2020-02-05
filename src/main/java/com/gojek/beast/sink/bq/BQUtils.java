package com.gojek.beast.sink.bq;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class BQUtils {
    /**
     * Compares fieldNames of fields and nested fields in the provided schemas.
     *
     * @param existingSchema
     * @param updatedSchema
     * @return false if there is a difference of fieldNames between the provided schemas
     * true if there fieldNames of the provided schemas are same
     */
    public static boolean compareBQSchemaFields(Schema existingSchema, Schema updatedSchema) {
        List<String> existingSchemaFieldNames = getFieldNames(existingSchema.getFields(), null);
        List<String> updatedSchemaFieldNames = getFieldNames(updatedSchema.getFields(), null);
        Collections.sort(existingSchemaFieldNames);
        Collections.sort(updatedSchemaFieldNames);
        return existingSchemaFieldNames.equals(updatedSchemaFieldNames);
    }

    private static List<String> getFieldNames(List<Field> fields, String parentFieldName) {
        List<String> fieldNames = new ArrayList<>();
        if (fields == null) {
            return fieldNames;
        }

        fields.stream().forEach(field -> {
            String fieldName = field.getName();
            if (parentFieldName != null) {
                fieldNames.add(parentFieldName + "." + fieldName);
            } else {
                fieldNames.add(fieldName);
            }
            fieldNames.addAll(getFieldNames(field.getSubFields(), fieldName));
        });
        return fieldNames;
    }
}
