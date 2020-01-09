package com.gojek.beast.protomapping;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.config.Constants;
import com.gojek.beast.exception.BQSchemaMappingException;
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
    private BigQuery bigquery;
    @Mock
    private BQConfig bqConfig;
    private BQClient bqClient;

    @Test
    public void shouldCreateBigqueryTableWithPartition() throws ProtoNotFoundException, BQSchemaMappingException {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getBQTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        bqClient.upsertTable(bqSchemaFields);

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithoutPartition() throws ProtoNotFoundException, BQSchemaMappingException {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        bqClient.upsertTable(bqSchemaFields);

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableAlreadyExists() throws ProtoNotFoundException, BQSchemaMappingException {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.create(tableInfo)).thenThrow(new BigQueryException(404, "Table Already Exists"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).update(tableInfo);
    }

    @Test(expected = BigQueryException.class)
    public void shouldThrowExceptionIfUpdateTableFails() throws ProtoNotFoundException, BQSchemaMappingException {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.create(tableInfo)).thenThrow(new BigQueryException(404, "Table Already Exists"));
        when(bigquery.update(tableInfo)).thenThrow(new BigQueryException(404, "Failed to update"));

        bqClient.upsertTable(bqSchemaFields);
    }

    private TableDefinition getPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        timePartitioningBuilder.setField(bqConfig.getBQTablePartitionKey());

        Schema schema = Schema.of(bqSchemaFields);

        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(timePartitioningBuilder.build())
                .build();
        return tableDefinition;
    }

    private TableDefinition getNonPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        Schema schema = Schema.of(bqSchemaFields);

        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        return tableDefinition;
    }
}
