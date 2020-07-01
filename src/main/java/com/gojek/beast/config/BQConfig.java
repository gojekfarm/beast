package com.gojek.beast.config;

import org.aeonbits.owner.Config;

import java.util.Map;

public interface BQConfig extends Config {
    @Key("BQ_PROJECT_NAME")
    String getGCPProject();

    @Key("BQ_TABLE_NAME")
    String getTable();

    @Key("BQ_DATASET_LABELS")
    @Separator(MapPropertyConverter.ELEMENT_SEPARATOR)
    @ConverterClass(MapPropertyConverter.class)
    Map<String, String> getDatasetLabels();

    @Key("BQ_TABLE_LABELS")
    @Separator(MapPropertyConverter.ELEMENT_SEPARATOR)
    @ConverterClass(MapPropertyConverter.class)
    Map<String, String> getTableLabels();

    @Key("BQ_DATASET_NAME")
    String getDataset();

    @Key("GOOGLE_CREDENTIALS")
    String getGoogleCredentials();

    @Key("ENABLE_BQ_TABLE_PARTITIONING")
    @DefaultValue("false")
    Boolean isBQTablePartitioningEnabled();

    @Key("BQ_TABLE_PARTITION_KEY")
    String getBQTablePartitionKey();

    @DefaultValue("true")
    @Key("ENABLE_BQ_ROW_INSERTID")
    Boolean isBQRowInsertIdEnabled();

    @DefaultValue("-1")
    @Key("BQ_CLIENT_READ_TIMEOUT")
    String getBqClientReadTimeout();

    @DefaultValue("-1")
    @Key("BQ_CLIENT_CONNECT_TIMEOUT")
    String getBqClientConnectTimeout();

    @DefaultValue("-1")
    @Key("BQ_TABLE_PARTITION_EXPIRY_MILLIS")
    Long getBQTablePartitionExpiryMillis();
}
