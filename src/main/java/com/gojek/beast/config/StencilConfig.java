package com.gojek.beast.config;

import org.aeonbits.owner.Config;

public interface StencilConfig extends Config {

    @Key("STENCIL_URL")
    String getStencilUrl();

    @Key("PROTO_SCHEMA")
    String getProtoSchema();

    @Key("REFRESH_CACHE")
    @DefaultValue("false")
    boolean getAutoRefreshCache();
}
