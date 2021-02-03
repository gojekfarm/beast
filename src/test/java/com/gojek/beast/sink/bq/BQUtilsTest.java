package com.gojek.beast.sink.bq;

import com.gojek.beast.config.Constants;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BQUtilsTest {

    @Test
    public void compareBQSchemaFieldIfSchemaIsNotChanged() {
        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        boolean areEqual = BQUtils.compareBQSchemaFields(Schema.of(bqSchemaFields), Schema.of(bqSchemaFields));
        assertTrue(areEqual);
    }

    @Test
    public void compareBQSchemaFieldIfSchemaIsChanged() {
        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>();
        updatedBQSchemaFields.addAll(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        boolean areEqual = BQUtils.compareBQSchemaFields(Schema.of(bqSchemaFields), Schema.of(updatedBQSchemaFields));
        assertFalse(areEqual);
    }
}
