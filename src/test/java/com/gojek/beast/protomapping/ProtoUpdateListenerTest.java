package com.gojek.beast.protomapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gojek.beast.TestKey;
import com.gojek.beast.config.*;
import com.gojek.beast.exception.BQDatasetLocationChangedException;
import com.gojek.beast.exception.BQTableUpdateFailure;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.beast.sink.bq.BQClient;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.models.DescriptorAndTypeName;
import com.gojek.de.stencil.utils.StencilUtils;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class ProtoUpdateListenerTest {
    @Mock
    private Converter protoMappingConverter;
    @Mock
    private Parser protoMappingParser;
    @Mock
    private BQClient bqInstance;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private ProtoFieldFactory protoFieldFactory;
    private ProtoMappingConfig protoMappingConfig;
    private StencilConfig stencilConfig;
    private ObjectMapper objectMapper;


    @Before
    public void setUp() {
        System.setProperty("PROTO_SCHEMA", "com.gojek.beast.TestKey");
        System.setProperty("ENABLE_AUTO_SCHEMA_UPDATE", "false");
        stencilConfig = ConfigFactory.create(StencilConfig.class, System.getProperties());
        protoMappingConfig = ConfigFactory.create(ProtoMappingConfig.class, System.getProperties());
        objectMapper = new ObjectMapper();
    }

    @Test
    public void shouldUseNewSchemaIfProtoChanges() throws IOException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKey.class.getName())).thenReturn(descriptorsMap.get(TestKey.class.getName()).getDescriptor());
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BQField.getMetadataFields());
        }};
        doNothing().when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);

        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals("order_number", actualNewProtoMapping.getProperty("1"));
        Assert.assertEquals("order_url", actualNewProtoMapping.getProperty("2"));
    }

    @Test(expected = BQTableUpdateFailure.class)
    public void shouldThrowExceptionIfParserFails() {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKey.class.getPackage(), TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);

        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenThrow(new ProtoNotFoundException("proto not found"));

        protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);
    }

    @Test(expected = BQTableUpdateFailure.class)
    public void shouldThrowExceptionIfConverterFails() throws IOException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);
        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKey.class.getPackage(), TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BQField.getMetadataFields());
        }};
        doThrow(new BigQueryException(10, "bigquery mapping has failed")).when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);
    }

    @Test(expected = BQTableUpdateFailure.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() throws IOException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s.%s", TestKey.class.getPackage(), TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BQField.getMetadataFields());
        }};
        doThrow(new BQDatasetLocationChangedException("cannot change dataset location")).when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldsWhenNamespaceIsNotProvided() throws IOException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKey.class.getName())).thenReturn(descriptorsMap.get(TestKey.class.getName()).getDescriptor());
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BQField.getMetadataFields()); // metadata fields are not namespaced
        }};
        doNothing().when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);

        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals("order_number", actualNewProtoMapping.getProperty("1"));
        Assert.assertEquals("order_url", actualNewProtoMapping.getProperty("2"));
        verify(bqInstance, times(1)).upsertTable(bqSchemaFields); // assert that metadata fields were not namespaced
    }

    @Test
    public void shouldNamespaceMetadataFieldsWhenNamespaceIsProvided() throws IOException {
        System.setProperty("BQ_METADATA_NAMESPACE", "metadata_ns");
        AppConfig appConfigTest = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListenerTest = new ProtoUpdateListener(new ConfigStore(appConfigTest, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(stencilClient.get(TestKey.class.getName())).thenReturn(descriptorsMap.get(TestKey.class.getName()).getDescriptor());
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(BQField.getNamespacedMetadataField(appConfigTest.getBqMetadataNamespace())); // metadata fields are namespaced
        }};
        doNothing().when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListenerTest.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);

        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals("order_number", actualNewProtoMapping.getProperty("1"));
        Assert.assertEquals("order_url", actualNewProtoMapping.getProperty("2"));
        verify(bqInstance, times(1)).upsertTable(bqSchemaFields);
        System.setProperty("BQ_METADATA_NAMESPACE", "");
    }

    @Test
    public void shouldThrowExceptionWhenMetadataNamespaceNameCollidesWithAnyFieldName() throws IOException {
        System.setProperty("BQ_METADATA_NAMESPACE", "order_number"); // set field name to an existing column name
        AppConfig appConfigTest = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListenerTest = new ProtoUpdateListener(new ConfigStore(appConfigTest, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField("order_url", 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(BQField.getNamespacedMetadataField(appConfigTest.getBqMetadataNamespace()));
        }};

        Exception exception = Assert.assertThrows(RuntimeException.class, () -> {
            protoUpdateListenerTest.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);
        });
        Assert.assertEquals("Error while updating bigquery table on callback:Metadata field(s) is already present in the schema. fields: [order_number]", exception.getMessage());
        verify(bqInstance, times(0)).upsertTable(bqSchemaFields);
        System.setProperty("BQ_METADATA_NAMESPACE", "");
    }

    @Test
    public void shouldThrowErrorWhenMetadataFieldsNameCollidesWithAnyOtherField() throws IOException {
        AppConfig appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());
        ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(new ConfigStore(appConfig, stencilConfig, protoMappingConfig, null), stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);

        ProtoField returnedProtoField = new ProtoField();
        String collidingColName = Constants.OFFSET_COLUMN_NAME;
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("order_number", 1));
        returnedProtoField.addField(new ProtoField(collidingColName, 2));

        HashMap<String, DescriptorAndTypeName> descriptorsMap = new HashMap<String, DescriptorAndTypeName>() {{
            put(String.format("%s", TestKey.class.getName()), new DescriptorAndTypeName(TestKey.getDescriptor(), String.format(".%s.%s", TestKey.getDescriptor().getFile().getPackage(), TestKey.getDescriptor().getName())));
        }};
        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), StencilUtils.getAllProtobufDescriptors(descriptorsMap), StencilUtils.getTypeNameToPackageNameMap(descriptorsMap))).thenReturn(returnedProtoField);
        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", collidingColName);
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(expectedProtoMapping);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(collidingColName, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(collidingColName, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            addAll(BQField.getMetadataFields()); // metadata fields are not namespaced
        }};

        Exception exception = Assert.assertThrows(RuntimeException.class, () -> {
            protoUpdateListener.onProtoUpdate(stencilConfig.getStencilUrl(), descriptorsMap);
        });
        Assert.assertEquals("Error while updating bigquery table on callback:Metadata field(s) is already present in the schema. fields: [message_offset]", exception.getMessage());
        verify(bqInstance, times(0)).upsertTable(bqSchemaFields);
    }
}
