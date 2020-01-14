package com.gojek.beast.protomapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gojek.beast.config.Constants;
import com.gojek.beast.exception.BQSchemaMappingException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.google.cloud.bigquery.Field;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Converter {
    private ObjectMapper objectMapper;

    public Converter() {
        objectMapper = new ObjectMapper();
    }

    public String generateColumnMappings(List<ProtoField> fields) throws IOException {
        ObjectNode objectNode = generateColumnMappingsJson(fields);
        return objectMapper.writeValueAsString(objectNode);
    }

    private ObjectNode generateColumnMappingsJson(List<ProtoField> fields) {
        if (fields.size() == 0) {
            return JsonNodeFactory.instance.objectNode();
        }

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        for (ProtoField field : fields) {
            if (field.isNested()) {
                ObjectNode innerJSONValue = generateColumnMappingsJson(field.getFields());
                innerJSONValue.put(Constants.Config.RECORD_NAME, field.getName());
                objNode.put(String.valueOf(field.getIndex()), innerJSONValue);
            } else {
                objNode.put(String.valueOf(field.getIndex()), field.getName());
            }
        }
        return objNode;
    }

    public List<Field> generateBigquerySchema(ProtoField protoField) throws BQSchemaMappingException {
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
