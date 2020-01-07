package com.gojek.beast.protomapping;

import com.gojek.beast.config.AppConfig;
import com.gojek.beast.exception.BigquerySchemaMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.de.stencil.client.StencilClient;
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
    private Converter converter;
    private Parser parser;
    private BigQuery bigquery;
    private String protoSchema;
    private StencilClient stencilClient;
    private TableId tableId;
    private AppConfig appConfig;
    private ProtoFieldFactory protoFieldFactory;

    public BQClient(Converter converter, Parser parser, BigQuery bigquery, String protoSchema, TableId tableId, AppConfig appConfig, ProtoFieldFactory protoFieldFactory) {
        this.converter = converter;
        this.parser = parser;
        this.bigquery = bigquery;
        this.protoSchema = protoSchema;
        this.tableId = tableId;
        this.appConfig = appConfig;
        this.protoFieldFactory = protoFieldFactory;
    }

    public void upsertTable() throws ProtoNotFoundException, BigquerySchemaMappingException {
        log.info("Upserting Bigquery table");

        ProtoField protoField = protoFieldFactory.getProtoField();
        protoField = parser.parseFields(protoField, protoSchema, stencilClient);
        List<Field> bqSchemaFields = converter.generateBigquerySchema(protoField);
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
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

    private TableDefinition getTableDefinition(Schema schema) {
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();


        if (appConfig.isBQTablePartitioningEnabled()) {
            TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
            timePartitioningBuilder.setField(appConfig.getBQTablePartitionKey());

            tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(schema)
                    .setTimePartitioning(timePartitioningBuilder.build())
                    .build();
        }
        return tableDefinition;
    }

    public void setStencilClient(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
    }
}
