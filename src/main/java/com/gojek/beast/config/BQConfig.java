package com.gojek.beast.config;

import org.aeonbits.owner.Config;

public interface BQConfig extends Config {
    @Key("BQ_PROJECT_NAME")
    String getGCPProject();

    @Key("BQ_TABLE_NAME")
    String getTable();

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
}
