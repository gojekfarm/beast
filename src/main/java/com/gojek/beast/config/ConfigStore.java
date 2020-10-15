package com.gojek.beast.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ConfigStore {
    private final AppConfig appConfig;
    private final StencilConfig stencilConfig;
    private final ProtoMappingConfig protoMappingConfig;
    private final BQConfig bqConfig;
}
