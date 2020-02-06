package com.gojek.beast.protomapping;

import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.*;
import com.google.type.Date;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ParserTest {
    @Mock
    private StencilClient stencilClient;
    private Parser protoMappingParser;

    @Before
    public void setup() {
        this.protoMappingParser = new Parser();
    }

    @Test(expected = ProtoNotFoundException.class)
    public void shouldThrowExceptionIfProtoNotFound() {
        protoMappingParser.parseFields(null, "beast.proto.shema", new HashMap<>(), new HashMap<>());
    }

    @Test(expected = ProtoNotFoundException.class)
    public void shouldThrowExceptionIfNestedProtoNotFound() {
        Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<String, Descriptors.Descriptor>() {{
            put("com.gojek.beast.TestMessage", TestMessage.getDescriptor());
        }};
        //when(stencilClient.getAll()).thenReturn(descriptorMap);
        ProtoField protoField = new ProtoField();
        protoMappingParser.parseFields(protoField, "com.gojek.beast.TestNestedMessage", descriptorMap, new HashMap<>());
    }

    @Test
    public void shouldParseProtoSchemaForNonNestedFields() {
        ArrayList<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

        fileDescriptors.add(TestMessage.getDescriptor().getFile());
        fileDescriptors.add(Duration.getDescriptor().getFile());
        fileDescriptors.add(Date.getDescriptor().getFile());
        fileDescriptors.add(Struct.getDescriptor().getFile());
        fileDescriptors.add(Timestamp.getDescriptor().getFile());

        Map<String, Descriptors.Descriptor> descriptorMap = getDescriptors(fileDescriptors);

        Map<String, String> typeNameToPackageNameMap = new HashMap<String, String>() {{
            put(".gojek.beast.TestMessage.CurrentStateEntry", "com.gojek.beast.TestMessage.CurrentStateEntry");
            put(".google.protobuf.Struct.FieldsEntry", "com.google.protobuf.Struct.FieldsEntry");
            put(".google.protobuf.Duration", "com.google.protobuf.Duration");
            put(".google.type.Date", "com.google.type.Date");
        }};
/*
        when(stencilClient.getAll()).thenReturn(descriptorMap);
        when(stencilClient.getTypeNameToPackageNameMap()).thenReturn(typeNameToPackageNameMap);*/
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, "com.gojek.beast.TestMessage", descriptorMap, typeNameToPackageNameMap);
        assertTestMessage(protoField.getFields());
    }

    @Test
    public void shouldParseProtoSchemaForNestedFields() {
        ArrayList<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

        fileDescriptors.add(TestMessage.getDescriptor().getFile());
        fileDescriptors.add(Duration.getDescriptor().getFile());
        fileDescriptors.add(Date.getDescriptor().getFile());
        fileDescriptors.add(Struct.getDescriptor().getFile());
        fileDescriptors.add(TestNestedMessage.getDescriptor().getFile());

        Map<String, Descriptors.Descriptor> descriptorMap = getDescriptors(fileDescriptors);

        Map<String, String> typeNameToPackageNameMap = new HashMap<String, String>() {{
            put(".gojek.beast.TestMessage.CurrentStateEntry", "com.gojek.beast.TestMessage.CurrentStateEntry");
            put(".google.protobuf.Struct.FieldsEntry", "com.google.protobuf.Struct.FieldsEntry");
            put(".google.protobuf.Duration", "com.google.protobuf.Duration");
            put(".google.type.Date", "com.google.type.Date");
            put(".gojek.beast.TestMessage", "com.gojek.beast.TestMessage");
        }};

        /*when(stencilClient.getAll()).thenReturn(descriptorMap);
        when(stencilClient.getTypeNameToPackageNameMap()).thenReturn(typeNameToPackageNameMap);*/
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, "com.gojek.beast.TestNestedMessage", descriptorMap, typeNameToPackageNameMap);
        assertField(protoField.getFields().get(0), "nested_id", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(protoField.getFields().get(1), "single_message", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);

        assertTestMessage(protoField.getFields().get(1).getFields());
    }

    private Map<String, Descriptors.Descriptor> getDescriptors(ArrayList<Descriptors.FileDescriptor> fileDescriptors) {
        Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<String, Descriptors.Descriptor>();
        fileDescriptors.stream().forEach(fd -> {
            String javaPackage = fd.getOptions().getJavaPackage();
            fd.getMessageTypes().stream().forEach(desc -> {
                String className = desc.getName();
                desc.getNestedTypes().stream().forEach(nestedDesc -> {
                    String nestedClassName = nestedDesc.getName();
                    descriptorMap.put(String.format("%s.%s.%s", javaPackage, className, nestedClassName), nestedDesc);
                });
                descriptorMap.put(String.format("%s.%s", javaPackage, className), desc);
            });
        });
        return descriptorMap;
    }

    private void assertTestMessage(List<ProtoField> fields) {
        assertEquals(14, fields.size());
        assertField(fields.get(0), "order_number", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(1), "order_url", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);
        assertField(fields.get(2), "order_details", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 3);
        assertField(fields.get(3), "created_at", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 4);
        assertField(fields.get(4), "status", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 5);
        assertField(fields.get(5), "discount", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 6);
        assertField(fields.get(6), "success", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 7);
        assertField(fields.get(7), "price", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 8);
        assertField(fields.get(8), "current_state", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 9);
        assertField(fields.get(9), "user_token", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 10);
        assertField(fields.get(10), "trip_duration", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 11);
        assertField(fields.get(11), "aliases", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 12);
        assertField(fields.get(12), "properties", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 13);
        assertField(fields.get(13), "order_date", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 14);

        assertEquals(String.format(".%s", Duration.getDescriptor().getFullName()), fields.get(10).getTypeName());
        assertEquals(2, fields.get(10).getFields().size());
        assertField(fields.get(10).getFields().get(0), "seconds", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(10).getFields().get(1), "nanos", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);

        assertEquals(String.format(".%s", Date.getDescriptor().getFullName()), fields.get(13).getTypeName());
        assertEquals(3, fields.get(13).getFields().size());
        assertField(fields.get(13).getFields().get(0), "year", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(13).getFields().get(1), "month", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);
        assertField(fields.get(13).getFields().get(2), "day", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 3);
    }

    private void assertField(ProtoField field, String name, DescriptorProtos.FieldDescriptorProto.Type ftype, DescriptorProtos.FieldDescriptorProto.Label flabel, int index) {
        assertEquals(name, field.getName());
        assertEquals(ftype, field.getType());
        assertEquals(flabel, field.getLabel());
        assertEquals(index, field.getIndex());
    }
}
