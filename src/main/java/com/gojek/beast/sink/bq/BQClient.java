package com.gojek.beast.sink.bq;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.exception.BQPartitionKeyNotSpecified;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.StandardTableDefinition;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
public class BQClient {
    private BigQuery bigquery;
    private TableId tableID;
    private Stats statsClient = Stats.client();
    private BQConfig bqConfig;
    private BQTableDefinition bqTableDefinition;

    public BQClient(BigQuery bigquery, BQConfig bqConfig) {
        this.bigquery = bigquery;
        this.bqConfig = bqConfig;
        this.tableID = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        this.bqTableDefinition = new BQTableDefinition(bqConfig);
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition)
                .setLabels(bqConfig.getTableLabels())
                .build();
        upsertDatasetAndTable(tableInfo);
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            bigquery.create(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            log.info("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
        } else if (!dataSet.getLabels().equals(bqConfig.getDatasetLabels())) {
            bigquery.update(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            log.info("Successfully UPDATED bigquery DATASET: {} with labels", tableID.getDataset());
        }

        Table table = bigquery.getTable(tableID);
        if (table == null || !table.exists()) {
            bigquery.create(tableInfo);
            log.info("Successfully CREATED bigquery TABLE: {}", tableID.getTable());
        } else {
            Schema existingSchema = table.getDefinition().getSchema();
            Schema updatedSchema = tableInfo.getDefinition().getSchema();

            if (shouldUpdateTable(tableInfo, table, existingSchema, updatedSchema)) {
                Instant start = Instant.now();
                bigquery.update(tableInfo);
                log.info("Successfully UPDATED bigquery TABLE: {}", tableID.getTable());
                statsClient.timeIt("bq.upsert.table.time," + statsClient.getBqTags(), start);
                statsClient.increment("bq.upsert.table.count," + statsClient.getBqTags());
            } else {
                log.info("Skipping bigquery table update, since proto schema hasn't changed");
            }
        }
    }

    private boolean shouldUpdateTable(TableInfo tableInfo, Table table, Schema existingSchema, Schema updatedSchema) {
        boolean needToChangePartitionExpiry = false;
        if (table.getDefinition().getType().equals(TableDefinition.Type.TABLE) && bqConfig.getBQTablePartitionExpiry() != -1) {
            if (shouldChangeParitionExpiryForStandardTable(table)) {
                needToChangePartitionExpiry = true;
            }
        }
        return !table.getLabels().equals(tableInfo.getLabels())
                || !BQUtils.compareBQSchemaFields(existingSchema, updatedSchema)
                || needToChangePartitionExpiry;
    }

    private boolean shouldChangeParitionExpiryForStandardTable(Table table) {
        Long expirationMs = ((StandardTableDefinition) (table.getDefinition())).getTimePartitioning().getExpirationMs();
        return expirationMs == null
                || (expirationMs.longValue() != bqConfig.getBQTablePartitionExpiry());
    }

    private TableDefinition getTableDefinition(Schema schema) throws BQPartitionKeyNotSpecified {
        return bqTableDefinition.getTableDefinition(schema);
    }
}
