package com.gojek.beast.protomapping;

import com.gojek.beast.exception.BigquerySchemaMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.de.stencil.client.StencilClient;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableInfo;
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

    public BQClient(Converter converter, Parser parser, BigQuery bigquery, String protoSchema, TableId tableId) {
        this.converter = converter;
        this.parser = parser;
        this.bigquery = bigquery;
        this.protoSchema = protoSchema;
        this.tableId = tableId;
    }

    public void upsertTable() throws ProtoNotFoundException, BigquerySchemaMappingException {
        log.info("Upserting Bigquery table");

        ProtoField protoField = new ProtoField();
        protoField = parser.parseFields(protoField, protoSchema, stencilClient);
        List<Field> bqSchemaFields = converter.generateBigquerySchema(protoField);
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
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

    public void setStencilClient(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
    }
}
