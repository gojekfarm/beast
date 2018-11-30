package com.gojek.beast.config;

import org.aeonbits.owner.Config;

public interface BqConfig extends Config {
    @Key("BQ_TABLE_NAME")
    String getTable();

    @Key("BQ_DATASET_NAME")
    String getDataset();

    @Key("GOOGLE_CREDENTIALS")
    String getGoogleCredentials();
}
