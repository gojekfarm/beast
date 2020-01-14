package com.gojek.beast.sink.bq;

import com.gojek.beast.config.BQConfig;
import com.gojek.beast.exception.BQPartitionKeyNotSpecified;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.LegacySQLTypeName;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class BQTableDefinition {
    private BQConfig bqConfig;

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        if (!bqConfig.isBQTablePartitioningEnabled()) {
            return tableDefinition;
        }
        return getPartitionedTableDefinition(schema);
    }

    private StandardTableDefinition getPartitionedTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinition = StandardTableDefinition.newBuilder();
        Optional<Field> partitionFieldOptional = schema.getFields().stream().filter(obj -> obj.getName().equals(bqConfig.getBQTablePartitionKey())).findFirst();
        if (!partitionFieldOptional.isPresent()) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key %s is not present in the schema", bqConfig.getBQTablePartitionKey()));
        }

        Field partitionField = partitionFieldOptional.get();
        if (isTimePartitionedField(partitionField)) {
            return createTimePartitionBuilder(partitionField, tableDefinition)
                    .setSchema(schema)
                    .build();
        } else {
            throw new UnsupportedOperationException("Range Bigquery partitioning is not supported, supported paritition fields have to be of DATE or TIMESTAMP type");
        }
    }

    private StandardTableDefinition.Builder createTimePartitionBuilder(Field partitionField, StandardTableDefinition.Builder tableBuilder) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        if (bqConfig.getBQTablePartitionKey() == null) {
            throw new BQPartitionKeyNotSpecified(String.format("Partition key not specified for the table: %s", bqConfig.getTable()));
        }
        timePartitioningBuilder.setField(bqConfig.getBQTablePartitionKey())
                .setRequirePartitionFilter(true);

        return tableBuilder
                .setTimePartitioning(timePartitioningBuilder.build());
    }

    private boolean isTimePartitionedField(Field partitionField) {
        return partitionField.getType() == LegacySQLTypeName.TIMESTAMP || partitionField.getType() == LegacySQLTypeName.DATE;
    }
}
