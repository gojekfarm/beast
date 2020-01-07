package com.gojek.beast.protomapping;

import com.gojek.beast.config.AppConfig;
import com.gojek.beast.exception.BigquerySchemaMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.google.cloud.bigquery.*;
import com.google.protobuf.DescriptorProtos;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BQClientTest {
    @Mock
    private Converter converter;
    @Mock
    private Parser parser;
    @Mock
    private BigQuery bigquery;
    @Mock
    private StencilClient stencilClient;
    @Mock
    private ProtoFieldFactory protoFieldFactory;
    private TableId tableId = TableId.of("dataset", "table");
    private String protoSchema = "com.beast.proto";
    private BQClient bqClient;
    private AppConfig appConfig;

    @Test
    public void shouldCreateBigqueryTableWithPartition() throws ProtoNotFoundException, BigquerySchemaMappingException {
        System.setProperty("ENABLE_BQ_TABLE_PARTITIONING", "true");
        System.setProperty("BQ_TABLE_PARTITION_KEY", "partition_column");
        appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());

        bqClient = new BQClient(converter, parser, bigquery, protoSchema, tableId, appConfig, protoFieldFactory);
        bqClient.setStencilClient(stencilClient);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("test-1", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        returnedProtoField.addField(new ProtoField("test-2", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        when(parser.parseFields(returnedProtoField, protoSchema, stencilClient)).thenReturn(returnedProtoField);
        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(converter.generateBigquerySchema(returnedProtoField)).thenReturn(bqSchemaFields);

        bqClient.upsertTable();

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithoutPartition() throws ProtoNotFoundException, BigquerySchemaMappingException {
        System.setProperty("ENABLE_BQ_TABLE_PARTITIONING", "false");
        appConfig = ConfigFactory.create(AppConfig.class, System.getProperties());

        bqClient = new BQClient(converter, parser, bigquery, protoSchema, tableId, appConfig, protoFieldFactory);
        bqClient.setStencilClient(stencilClient);

        ProtoField returnedProtoField = new ProtoField();
        when(protoFieldFactory.getProtoField()).thenReturn(returnedProtoField);
        returnedProtoField.addField(new ProtoField("test-1", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        returnedProtoField.addField(new ProtoField("test-2", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        when(parser.parseFields(returnedProtoField, protoSchema, stencilClient)).thenReturn(returnedProtoField);
        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        when(converter.generateBigquerySchema(returnedProtoField)).thenReturn(bqSchemaFields);

        bqClient.upsertTable();

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        verify(bigquery).create(tableInfo);
    }

    private TableDefinition getPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        timePartitioningBuilder.setField(appConfig.getBQTablePartitionKey());
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        Schema schema = Schema.of(bqSchemaFields);

        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(timePartitioningBuilder.build())
                .build();
        return tableDefinition;
    }

    private TableDefinition getNonPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        Schema schema = Schema.of(bqSchemaFields);

        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        return tableDefinition;
    }
}
