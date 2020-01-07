package com.gojek.beast.protomapping;

import com.gojek.beast.Clock;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.exception.BQTableUpdateFailure;
import com.gojek.beast.exception.BigquerySchemaMappingException;
import com.gojek.beast.exception.ProtoMappingException;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ProtoUpdateListener extends com.gojek.de.stencil.cache.ProtoUpdateListener {
    private final String proto;
    private final ProtoMappingConfig protoMappingConfig;
    private final AppConfig appConfig;
    private ConsumerRecordConverter recordConverter;
    private StencilClient stencilClient;
    private Converter protoMappingConverter;
    private Parser protoMappingParser;
    private BQClient bqClient;
    private ProtoFieldFactory protoFieldFactory;

    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, AppConfig appConfig, Converter protoMappingConverter, Parser protoMappingParser, BigQuery bqInstance, TableId tableId) {
        super(appConfig.getProtoSchema());
        this.proto = appConfig.getProtoSchema();
        this.protoMappingConfig = protoMappingConfig;
        this.appConfig = appConfig;
        this.protoMappingConverter = protoMappingConverter;
        this.protoMappingParser = protoMappingParser;
        this.protoFieldFactory = new ProtoFieldFactory();
        this.bqClient = new BQClient(protoMappingConverter, protoMappingParser, bqInstance, proto, tableId, appConfig, protoFieldFactory);
        this.createStencilClient();
        this.setProtoParser(getProtoMapping());
    }

    @VisibleForTesting
    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, AppConfig appConfig, StencilClient stencilClient, Converter protoMappingConverter, Parser protoMappingParser, BQClient bqClient, ProtoFieldFactory protoFieldFactory) {
        super(appConfig.getProtoSchema());
        this.proto = appConfig.getProtoSchema();
        this.protoMappingConfig = protoMappingConfig;
        this.appConfig = appConfig;
        this.stencilClient = stencilClient;
        this.protoMappingConverter = protoMappingConverter;
        this.protoMappingParser = protoMappingParser;
        this.bqClient = bqClient;
        this.protoFieldFactory = protoFieldFactory;
    }

    private void createStencilClient() {
        if (protoMappingConfig.isAutoSchemaUpdateEnabled()) {
            stencilClient = StencilClientFactory.getClient(appConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient(), this);
            this.bqClient.setStencilClient(stencilClient);
            log.info("updating bq table at startup for proto schema {}", getProto());
            try {
                updateProtoParser();
            } catch (ProtoNotFoundException | BigquerySchemaMappingException e) {
                String errMsg = "Error while updating bigquery table:" + e.getMessage();
                log.error(errMsg);
                e.printStackTrace();
                throw new BQTableUpdateFailure(errMsg);
            }

        } else {
            stencilClient = StencilClientFactory.getClient(appConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient());
        }
    }

    @Override
    public void onProtoUpdate() {
        log.info("updating bq table as {} proto was updated", getProto());
        try {
            updateProtoParser();
        } catch (ProtoNotFoundException | BigquerySchemaMappingException | BigQueryException e) {
            String errMsg = "Error while updating bigquery table:" + e.getMessage();
            log.error(errMsg);
            e.printStackTrace();
            throw new BQTableUpdateFailure(errMsg);
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser() throws ProtoNotFoundException, BigquerySchemaMappingException, BigQueryException {
        ProtoField protoField = protoFieldFactory.getProtoField();
        protoField = protoMappingParser.parseFields(protoField, proto, stencilClient);
        JsonObject protoMappingJson = protoMappingConverter.generateColumnMappings(protoField.getFields());

        bqClient.upsertTable();
        protoMappingConfig.setProperty("PROTO_COLUMN_MAPPING", protoMappingJson.toString());
        setProtoParser(protoMappingConfig.getProtoColumnMapping());
    }

    private ColumnMapping getProtoMapping() {
        if (protoMappingConfig.getProtoColumnMappingURL() != null) {
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
        }
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
