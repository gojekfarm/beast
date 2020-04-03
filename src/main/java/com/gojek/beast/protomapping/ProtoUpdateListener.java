package com.gojek.beast.protomapping;

import com.gojek.beast.Clock;
import com.gojek.beast.config.BQConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.config.StencilConfig;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.exception.BQTableUpdateFailure;
import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.BQField;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.models.ProtoFieldFactory;
import com.gojek.beast.sink.bq.BQClient;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.models.DescriptorAndTypeName;
import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.de.stencil.utils.StencilUtils;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    private Stats statsClient = Stats.client();

    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, StencilConfig stencilConfig, BQConfig bqConfig, Converter protoMappingConverter, Parser protoMappingParser, BigQuery bqInstance) throws IOException {
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

            log.info("updating bq table at startup for proto schema {}", getProto());
            onProtoUpdate(stencilConfig.getStencilUrl(), stencilClient.getAllDescriptorAndTypeName());
        } else {
            stencilClient = StencilClientFactory.getClient(stencilConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient());
        }
    }

    @Override
    public void onProtoUpdate(String url, Map<String, DescriptorAndTypeName> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            ProtoField protoField = protoFieldFactory.getProtoField();
            protoField = protoMappingParser.parseFields(protoField, proto, StencilUtils.getAllProtobufDescriptors(newDescriptors), StencilUtils.getTypeNameToPackageNameMap(newDescriptors));
            updateProtoParser(protoField);
        } catch (BigQueryException | ProtoNotFoundException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            statsClient.increment("bq.table.upsert.failures");
            throw new BQTableUpdateFailure(errMsg);
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser(final ProtoField protoField) throws IOException {
        String protoMappingString = protoMappingConverter.generateColumnMappings(protoField.getFields());
        List<Field> bqSchemaFields = protoMappingConverter.generateBigquerySchema(protoField);
        List<Field> bqMetadataFieldsSchema = BQField.getMetadataFields();
        bqSchemaFields.addAll(bqMetadataFieldsSchema);

        bqClient.upsertTable(bqSchemaFields);
        protoMappingConfig.setProperty("PROTO_COLUMN_MAPPING", protoMappingString);
        setProtoParser(protoMappingConfig.getProtoColumnMapping());
    }

    private ColumnMapping getProtoMapping() throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, proto, stencilClient.getAll(), stencilClient.getTypeNameToPackageNameMap());
        String protoMapping = protoMappingConverter.generateColumnMappings(protoField.getFields());
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
