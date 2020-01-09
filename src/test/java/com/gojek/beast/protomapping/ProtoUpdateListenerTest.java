package com.gojek.beast.protomapping;

import com.gojek.beast.config.*;
import com.gojek.beast.exception.BQTableUpdateFailure;
import com.gojek.beast.exception.BQSchemaMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.gson.JsonObject;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

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
    private ProtoUpdateListener protoUpdateListener;
    private ProtoMappingConfig protoMappingConfig;
    private StencilConfig stencilConfig;


    @Before
    public void setUp() {
        System.setProperty("PROTO_SCHEMA", "com.gojek.beast");
        System.setProperty("ENABLE_AUTO_SCHEMA_UPDATE", "true");
        stencilConfig = ConfigFactory.create(StencilConfig.class, System.getProperties());
        protoMappingConfig = ConfigFactory.create(ProtoMappingConfig.class, System.getProperties());
        protoUpdateListener = new ProtoUpdateListener(protoMappingConfig, stencilConfig, stencilClient, protoMappingConverter, protoMappingParser, bqInstance, protoFieldFactory);
    }

    @Test
    public void shouldUseNewSchemaIfProtoChanges() throws ProtoNotFoundException, BQSchemaMappingException {
        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("test-1", 1));
        returnedProtoField.addField(new ProtoField("test-2", 2));

        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), stencilClient)).thenReturn(returnedProtoField);
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("1", "test-1");
        jsonObj.addProperty("2", "test-2");
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(jsonObj);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        doNothing().when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate();

        ColumnMapping actualNewProtoMapping = protoMappingConfig.getProtoColumnMapping();
        Assert.assertEquals("test-1", actualNewProtoMapping.getProperty("1"));
        Assert.assertEquals("test-2", actualNewProtoMapping.getProperty("2"));
    }

    @Test(expected = BQTableUpdateFailure.class)
    public void shouldThrowExceptionIfParserFails() throws ProtoNotFoundException {
        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("test-1", 1));
        returnedProtoField.addField(new ProtoField("test-2", 2));

        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), stencilClient)).thenThrow(new ProtoNotFoundException("proto not found"));

        protoUpdateListener.onProtoUpdate();
    }

    @Test(expected = BQTableUpdateFailure.class)
    public void shouldThrowExceptionIfConverterFails() throws ProtoNotFoundException, BQSchemaMappingException {
        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("test-1", 1));
        returnedProtoField.addField(new ProtoField("test-2", 2));

        when(protoMappingParser.parseFields(returnedProtoField, stencilConfig.getProtoSchema(), stencilClient)).thenReturn(returnedProtoField);
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("1", "test-1");
        jsonObj.addProperty("2", "test-2");
        when(protoMappingConverter.generateColumnMappings(returnedProtoField.getFields())).thenReturn(jsonObj);

        ArrayList<Field> returnedSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(protoMappingConverter.generateBigquerySchema(returnedProtoField)).thenReturn(returnedSchemaFields);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        doThrow(new BigQueryException(10, "bigquery mapping has failed")).when(bqInstance).upsertTable(bqSchemaFields);

        protoUpdateListener.onProtoUpdate();
    }
}
