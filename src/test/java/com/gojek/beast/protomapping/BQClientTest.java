package com.gojek.beast.protomapping;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.config.Constants;
import com.gojek.beast.sink.bq.BQClient;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BQClientTest {
    @Mock
    private BigQuery bigquery;
    @Mock
    private BQConfig bqConfig;
    @Mock
    private Dataset dataset;
    @Mock
    private Table table;
    private BQClient bqClient;

    @Test
    public void shouldIgnoreExceptionIfDatasetAlreadyExists() {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getBQTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(false);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(DatasetInfo.of(tableId.getDataset()));
        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithPartition() {
        when(bqConfig.isBQTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getBQTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getTable()).thenReturn("bq-table");
        when(bqConfig.getDataset()).thenReturn("bq-proto");
        bqClient = new BQClient(bigquery, bqConfig);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithoutPartition() {
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
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableAlreadyExists() {
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
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(bigquery.create(tableInfo)).thenThrow(new BigQueryException(404, "Table Already Exists"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).update(tableInfo);
    }

    @Test(expected = BigQueryException.class)
    public void shouldThrowExceptionIfUpdateTableFails() {
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
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(bigquery.create(tableInfo)).thenThrow(new BigQueryException(404, "Table Already Exists"));
        when(bigquery.update(tableInfo)).thenThrow(new BigQueryException(404, "Failed to update"));

        bqClient.upsertTable(bqSchemaFields);
    }

    private TableDefinition getPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        timePartitioningBuilder.setField(bqConfig.getBQTablePartitionKey())
                .setRequirePartitionFilter(true);

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
