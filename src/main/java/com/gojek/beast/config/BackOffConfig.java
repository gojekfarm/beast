package com.gojek.beast.config;

import org.aeonbits.owner.Config;
public interface BackOffConfig extends Config {

    @Key("EXPONENTIAL_BACKOFF_INITIAL_BACKOFF_IN_MS")
    @DefaultValue("10")
    Integer getExponentialBackoffInitialTimeInMs();

    @Key("EXPONENTIAL_BACKOFF_MAXIMUM_BACKOFF_IN_MS")
    @DefaultValue("60000")
    Integer getExponentialBackoffMaximumTimeInMs();

    @Key("EXPONENTIAL_BACKOFF_RATE")
    @DefaultValue("2")
    Integer getExponentialBackoffRate();
}
