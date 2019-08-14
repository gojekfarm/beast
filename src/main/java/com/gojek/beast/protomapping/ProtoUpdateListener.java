package com.gojek.beast.protomapping;

import com.gojek.beast.Clock;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.config.ProtoMappingConfig;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.models.ExternalCallException;
import com.gojek.beast.models.UpdateBQTableRequest;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.ProtoParser;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ProtoUpdateListener extends com.gojek.de.stencil.cache.ProtoUpdateListener {
    private final String proto;
    private final ProtoMappingConfig protoMappingConfig;
    private final AppConfig appConfig;
    private ConsumerRecordConverter recordConverter;
    private StencilClient stencilClient;
    private UpdateTableService updateTableService;

    public ProtoUpdateListener(ProtoMappingConfig protoMappingConfig, AppConfig appConfig, UpdateTableService updateTableService) throws ExternalCallException {
        super(appConfig.getProtoSchema());
        this.proto = appConfig.getProtoSchema();
        this.protoMappingConfig = protoMappingConfig;
        this.appConfig = appConfig;
        this.updateTableService = updateTableService;
        this.createStencilClient();
        this.setProtoParser(getProtoMapping());
    }

    private void createStencilClient() {
        if (protoMappingConfig.isAutoSchemaUpdateEnabled()) {
            stencilClient = StencilClientFactory.getClient(appConfig.getStencilUrl(), System.getenv(), Stats.client().getStatsDClient(), this);

            log.info("updating bq table at startup", getProto());
            try {
                updateProtoParser();
            } catch (ExternalCallException e) {
                log.warn("Error while updating bigquery table:" + e.getMessage());
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
        } catch (ExternalCallException e) {
            log.warn("Error while updating bigquery table:" + e.getMessage());
        }
    }

    // First get latest protomapping, update bq schema, and if all goes fine
    // then only update beast's proto mapping config
    private void updateProtoParser() throws ExternalCallException {
        UpdateBQTableRequest request = new UpdateBQTableRequest(appConfig.getGCPProject(), proto, appConfig.getTable(), appConfig.getDataset());
        String protoMapping = updateTableService.getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), proto);
        updateTableService.updateBigQuerySchema(protoMappingConfig.getUpdateBQTableURL(), request);
        protoMappingConfig.setProperty("PROTO_COLUMN_MAPPING", protoMapping);
        setProtoParser(protoMappingConfig.getProtoColumnMapping());
    }

    private ColumnMapping getProtoMapping() throws ExternalCallException {
        if (protoMappingConfig.getProtoColumnMappingURL() != null) {
            String protoMapping = updateTableService.getProtoMappingFromRemoteURL(protoMappingConfig.getProtoColumnMappingURL(), proto);
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
