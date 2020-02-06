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
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Dataset;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
public class BQClient {
    private BigQuery bigquery;
    private TableId tableID;
    private Stats statsClient = Stats.client();
    private BQTableDefinition bqTableDefinition;

    public BQClient(BigQuery bigquery, BQConfig bqConfig) {
        this.bigquery = bigquery;
        this.tableID = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        this.bqTableDefinition = new BQTableDefinition(bqConfig);
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition).build();
        upsertDatasetAndTable(tableInfo);
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            bigquery.create(DatasetInfo.of(tableID.getDataset()));
            log.info("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
        }
        Table table = bigquery.getTable(tableID);
        if (table == null || !table.exists()) {
            bigquery.create(tableInfo);
            log.info("Successfully CREATED bigquery TABLE: {}", tableID.getTable());
        } else {
            Schema existingSchema = table.getDefinition().getSchema();
            Schema updatedSchema = tableInfo.getDefinition().getSchema();

            if (!BQUtils.compareBQSchemaFields(existingSchema, updatedSchema)) {
                Instant start = Instant.now();
                bigquery.update(tableInfo);
                log.info("Successfully UPDATED bigquery TABLE: {}", tableID.getTable());
                statsClient.timeIt("bq.upsert.table.time," + statsClient.getBqTags(), start);
                statsClient.increment("bq.upsert.table.count," + statsClient.getBqTags());
            }
        }
    }

    private TableDefinition getTableDefinition(Schema schema) throws BQPartitionKeyNotSpecified {
        return bqTableDefinition.getTableDefinition(schema);
    }
}
