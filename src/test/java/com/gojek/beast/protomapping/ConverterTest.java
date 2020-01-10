package com.gojek.beast.protomapping;

import com.gojek.beast.config.Constants;
import com.gojek.beast.models.ProtoField;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.DescriptorProtos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConverterTest {

    private Converter converter = new Converter();

    private Map<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName> expectedType = new HashMap<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName>() {{
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, LegacySQLTypeName.BOOLEAN);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, LegacySQLTypeName.FLOAT);
    }};

    @Test
    public void shouldTestShouldCreateFirstLevelColumnMappingSuccessfully() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("order_number", 1));
            add(new ProtoField("order_url", 2));
            add(new ProtoField("order_details", 3));
            add(new ProtoField("created_at", 4));
            add(new ProtoField("status", 5));
        }});

        JsonObject expectedMapping = new JsonObject();
        expectedMapping.addProperty("1", "order_number");
        expectedMapping.addProperty("2", "order_url");
        expectedMapping.addProperty("3", "order_details");
        expectedMapping.addProperty("4", "created_at");
        expectedMapping.addProperty("5", "status");

        JsonObject columnMapping = converter.generateColumnMappings(protoField.getFields());

        assertEquals(expectedMapping.toString(), columnMapping.toString());
    }

    @Test
    public void shouldTestShouldCreateNestedMapping() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("order_number", 1));
            add(new ProtoField("order_url", "some.type.name", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, 2, new ArrayList<ProtoField>() {{
                add(new ProtoField("host", 1));
                add(new ProtoField("url", 2));
            }}));
            add(new ProtoField("order_details", 3));
        }});

        Gson gson = new Gson();
        JsonObject expectedMapping = new JsonObject();
        JsonObject innerMapping = new JsonObject();
        innerMapping.addProperty("1", "host");
        innerMapping.addProperty("2", "url");
        innerMapping.addProperty("record_name", "order_url");
        expectedMapping.addProperty("1", "order_number");
        expectedMapping.add("2", gson.fromJson(innerMapping.toString(), JsonElement.class));
        expectedMapping.addProperty("3", "order_details");


        JsonObject columnMapping = converter.generateColumnMappings(protoField.getFields());

        assertEquals(expectedMapping.toString(), columnMapping.toString());
    }

    @Test
    public void generateColumnMappingsForNoFields() {
        JsonObject json = converter.generateColumnMappings(new ArrayList<>());
        assertEquals(json.size(), 0);
    }

    @Test
    public void shouldTestConvertToSchemaSuccessful() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(new ProtoField("field0_bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field1_string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field2_bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field3_enum", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field4_double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field5_float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));


        List<Field> fields = converter.generateBigquerySchema(new ProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(expectedType.get(nestedBQFields.get(index).getType()), fields.get(index).getType());
                });

    }

    @Test
    public void shouldTestShouldConvertIntegerDataTypes() {
        List<DescriptorProtos.FieldDescriptorProto.Type> allIntTypes = new ArrayList<DescriptorProtos.FieldDescriptorProto.Type>() {{
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        }};

        List<ProtoField> nestedBQFields = IntStream.range(0, allIntTypes.size())
                .mapToObj(index -> new ProtoField("field-" + index, allIntTypes.get(index), DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .collect(Collectors.toList());


        List<Field> fields = converter.generateBigquerySchema(new ProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(LegacySQLTypeName.INTEGER, fields.get(index).getType());
                });
    }

    @Test
    public void shouldTestShouldConvertNestedField() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(new ProtoField("field1_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field2_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    nestedBQFields));
        }});


        List<Field> fields = converter.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(nestedBQFields.size(), fields.get(1).getSubFields().size());

        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(nestedBQFields.get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(0));
        assertBqField(nestedBQFields.get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(1));

    }

    @Test
    public void shouldTestShouldConvertMultiNestedFields() {
        List<ProtoField> nestedBQFields = new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }};

        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(new ProtoField(
                    "field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {{
                        add(new ProtoField(
                                "field1_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(new ProtoField(
                                "field2_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                        add(new ProtoField(
                                "field3_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(new ProtoField(
                                "field4_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                    }}
            ));
        }});

        List<Field> fields = converter.generateBigquerySchema(protoField);


        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(4, fields.get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(3).getSubFields().size());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(1).getSubFields());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(3).getSubFields());
    }

    @Test
    public void shouldTestConvertToSchemaForTimestamp() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_timestamp",
                    Constants.ProtobufTypeName.TIMESTAMP_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }});

        List<Field> fields = converter.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.TIMESTAMP, Field.Mode.NULLABLE, fields.get(0));
    }

    @Test
    public void shouldTestConvertToSchemaForSpecialFields() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_struct",
                    Constants.ProtobufTypeName.STRUCT_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_bytes",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(new ProtoField("field3_duration",
                    "." + com.google.protobuf.Duration.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(new ProtoField("duration_seconds",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("duration_nanos",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

            add(new ProtoField("field3_date",
                    "." + com.google.type.Date.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(new ProtoField("year",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("month",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("date",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

        }});

        List<Field> fields = converter.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(protoField.getFields().get(2).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(2));
        assertBqField(protoField.getFields().get(3).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(3));
        assertEquals(2, fields.get(2).getSubFields().size());
        assertBqField("duration_seconds", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(0));
        assertBqField("duration_nanos", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(1));
    }


    @Test
    public void shouldTestConverterToSchemaForNullFields() {
        List<Field> fields = converter.generateBigquerySchema(null);
        assertNull(fields);
    }

    @Test
    public void shouldTestConvertToSchemaForRepeatedFields() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_map",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));
            add(new ProtoField("field2_repeated",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));

        }});

        List<Field> fields = converter.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.INTEGER, Field.Mode.REPEATED, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.REPEATED, fields.get(1));
    }

    public void assertMultipleFields(List<ProtoField> pfields, List<Field> bqFields) {
        IntStream.range(0, bqFields.size())
                .forEach(index -> {
                    assertBqField(pfields.get(index).getName(), expectedType.get(pfields.get(index).getType()), Field.Mode.NULLABLE, bqFields.get(index));
                });
    }

    public void assertBqField(String name, LegacySQLTypeName ftype, Field.Mode mode, Field bqf) {
        assertEquals(mode, bqf.getMode());
        assertEquals(name, bqf.getName());
        assertEquals(ftype, bqf.getType());
    }
}
