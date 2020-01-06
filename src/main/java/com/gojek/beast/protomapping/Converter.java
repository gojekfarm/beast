package com.gojek.beast.protomapping;

import com.gojek.beast.exception.BigquerySchemaMappingException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.google.cloud.bigquery.Field;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class Converter {
    public JsonObject generateColumnMappings(List<ProtoField> fields) {
        if (fields.size() == 0) {
            return new JsonObject();
        }
        JsonObject json = new JsonObject();
        fields.stream().forEach(field -> {
            if (field.isNested()) {
                JsonObject innerJSONValue = generateColumnMappings(field.getFields());
                innerJSONValue.addProperty("record_name", field.getName());

                Gson gson = new Gson();
                json.add(String.valueOf(field.getIndex()), gson.fromJson(innerJSONValue.toString(), JsonElement.class));
            } else {
                json.addProperty(String.valueOf(field.getIndex()), field.getName());
            }
        });
        return json;
    }

    public List<Field> generateBigquerySchema(ProtoField protoField) throws BigquerySchemaMappingException {
        if (protoField == null) {
            return null;
        }
        List<Field> schemaFields = new ArrayList<>();
        for (ProtoField field : protoField.getFields()) {
            BQField bqField = new BQField(field);
            if (field.isNested()) {
                List<Field> fields = generateBigquerySchema(field);
                bqField.setSubFields(fields);
            }
            schemaFields.add(bqField.getField());
        }
        return schemaFields;
    }
}
