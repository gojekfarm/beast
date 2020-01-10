package com.gojek.beast.sink.bq;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.exception.BQPartitionKeyNotSpecified;
import com.gojek.beast.stats.Stats;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.DatasetInfo;
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
        log.info("Upserting Bigquery table");
        Instant start = Instant.now();
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition).build();

        upsertDatasetAndStream(tableInfo);
        statsClient.timeIt("bq.upsert.table.time", start);
        log.info("Successfully upserted bigquery table");
    }

    private void upsertDatasetAndStream(TableInfo tableInfo) {
        try {
            bigquery.create(DatasetInfo.of(tableID.getDataset()));
        } catch (BigQueryException e) {
            log.info("Bigquery dataset already exists, no need to create dataset");
        }

        try {
            bigquery.create(tableInfo);
        } catch (BigQueryException e) {
            if (e.getMessage().contains("Already Exists")) {
                bigquery.update(tableInfo);
            } else {
                throw e;
            }
        }
    }

    private TableDefinition getTableDefinition(Schema schema) throws BQPartitionKeyNotSpecified {
        return bqTableDefinition.getTableDefinition(schema);
    }
}
