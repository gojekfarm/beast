package com.gojek.beast.protomapping;

import com.gojek.beast.Clock;
import com.gojek.beast.config.StencilConfig;
import com.gojek.beast.config.BQConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.exception.BQTableUpdateFailure;
import com.gojek.beast.exception.BQSchemaMappingException;
import com.gojek.beast.exception.ProtoMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@Slf4j
public class ProtoUpdateListener extends com.gojek.de.stencil.cache.ProtoUpdateListener {
    private final String proto;
    private final ProtoMappingConfig protoMappingConfig;
    private final StencilConfig stencilConfig;
    private ConsumerRecordConverter recordConverter;
    private StencilClient stencilClient;
    private Converter protoMappingConverter;
    private Parser protoMappingParser;
    private BQClient bqClient;
    private ProtoFieldFactory protoFieldFactory;

    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, StencilConfig stencilConfig, BQConfig bqConfig, Converter protoMappingConverter, Parser protoMappingParser, BigQuery bqInstance) {
        super(stencilConfig.getProtoSchema());
        this.proto = stencilConfig.getProtoSchema();
        this.protoMappingConfig = protoMappingConfig;
        this.stencilConfig = stencilConfig;
        this.protoMappingConverter = protoMappingConverter;
        this.protoMappingParser = protoMappingParser;
        this.protoFieldFactory = new ProtoFieldFactory();
        this.bqClient = new BQClient(bqInstance, bqConfig);
        this.createStencilClient();
        this.setProtoParser(getProtoMapping());
    }

    @VisibleForTesting
    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, StencilConfig stencilConfig, StencilClient stencilClient, Converter protoMappingConverter, Parser protoMappingParser, BQClient bqClient, ProtoFieldFactory protoFieldFactory) {
        super(stencilConfig.getProtoSchema());
        this.proto = stencilConfig.getProtoSchema();
        this.protoMappingConfig = protoMappingConfig;
        this.stencilConfig = stencilConfig;
        this.stencilClient = stencilClient;
        this.protoMappingConverter = protoMappingConverter;
        this.protoMappingParser = protoMappingParser;
        this.bqClient = bqClient;
        this.protoFieldFactory = protoFieldFactory;
    }

    private void createStencilClient() {
        if (protoMappingConfig.isAutoSchemaUpdateEnabled()) {
            stencilClient = StencilClientFactory.getClient(stencilConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient(), this);
        } else {
            stencilClient = StencilClientFactory.getClient(stencilConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient());
        }

        log.info("updating bq table at startup for proto schema {}", getProto());
        try {
            updateProtoParser();
        } catch (ProtoNotFoundException | BQSchemaMappingException e) {
            String errMsg = "Error while updating bigquery table:" + e.getMessage();
            log.error(errMsg);
            e.printStackTrace();
            throw new BQTableUpdateFailure(errMsg);
        }
    }

    @Override
    public void onProtoUpdate() {
        log.info("updating bq table as {} proto was updated", getProto());
        try {
            updateProtoParser();
        } catch (ProtoNotFoundException | BQSchemaMappingException | BigQueryException e) {
            String errMsg = "Error while updating bigquery table:" + e.getMessage();
            log.error(errMsg);
            e.printStackTrace();
            throw new BQTableUpdateFailure(errMsg);
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser() throws ProtoNotFoundException, BQSchemaMappingException, BigQueryException {
        ProtoField protoField = protoFieldFactory.getProtoField();
        protoField = protoMappingParser.parseFields(protoField, proto, stencilClient);
        JsonObject protoMappingJson = protoMappingConverter.generateColumnMappings(protoField.getFields());

        List<Field> bqSchemaFields = protoMappingConverter.generateBigquerySchema(protoField);
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        bqClient.upsertTable(bqSchemaFields);
        protoMappingConfig.setProperty("PROTO_COLUMN_MAPPING", protoMappingJson.toString());
        setProtoParser(protoMappingConfig.getProtoColumnMapping());
    }

    private ColumnMapping getProtoMapping() {
        ProtoField protoField = new ProtoField();
        try {
            protoField = protoMappingParser.parseFields(protoField, proto, stencilClient);
        } catch (ProtoNotFoundException e) {
            String errMsg = "Error while generating proto to column mapping:" + e.getMessage();
            log.error(errMsg);
            e.printStackTrace();
            throw new ProtoMappingException(errMsg);
        }
        JsonObject protoMappingJson = protoMappingConverter.generateColumnMappings(protoField.getFields());
        String protoMapping = protoMappingJson.toString();
        protoMappingConfig.setProperty("PROTO_COLUMN_MAPPING", protoMapping);
        return protoMappingConfig.getProtoColumnMapping();
    }


    public ConsumerRecordConverter getProtoParser() {
        return recordConverter;
    }

    private void setProtoParser(ColumnMapping columnMapping) {
        ProtoParser protoParser = new ProtoParser(stencilClient, proto);
        recordConverter = new ConsumerRecordConverter(new RowMapper(columnMapping), protoParser, new Clock());
    }

    public void close() throws IOException {
        stencilClient.close();
    }
}
