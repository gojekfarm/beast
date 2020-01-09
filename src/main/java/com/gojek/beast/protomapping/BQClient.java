package com.gojek.beast.protomapping;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.exception.BQPartitionKeyNotSpecified;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class BQClient {
    private BigQuery bigquery;
    private TableId tableID;
    private BQConfig bqConfig;

    public BQClient(BigQuery bigquery, BQConfig bqConfig) {
        this.bigquery = bigquery;
        this.tableID = TableId.of(bqConfig.getDataset(), bqConfig.getTable());
        this.bqConfig = bqConfig;
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        log.info("Upserting Bigquery table");
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition).build();
        try {
            bigquery.create(tableInfo);
        } catch (BigQueryException e) {
            if (e.getMessage().contains("Already Exists")) {
                bigquery.update(tableInfo);
            } else {
                throw e;
            }
        }
        log.info("Successfully upserted bigquery table");
    }

    private TableDefinition getTableDefinition(Schema schema) throws BQPartitionKeyNotSpecified {
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();


        if (bqConfig.isBQTablePartitioningEnabled()) {
            TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
            if (bqConfig.getBQTablePartitionKey() == null) {
                throw new BQPartitionKeyNotSpecified(String.format("Partition key not specified for the table: %s", bqConfig.getTable()));
            }
            timePartitioningBuilder.setField(bqConfig.getBQTablePartitionKey());

            tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(schema)
                    .setTimePartitioning(timePartitioningBuilder.build())
                    .build();
        }
        return tableDefinition;
    }
}
