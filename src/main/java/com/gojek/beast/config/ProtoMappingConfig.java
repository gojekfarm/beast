package com.gojek.beast.config;

import org.aeonbits.owner.Mutable;

public interface ProtoMappingConfig extends Mutable {
    @Key("PROTO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    ColumnMapping getProtoColumnMapping();

    @DefaultValue("false")
    @Key("ENABLE_AUTO_SCHEMA_UPDATE")
    Boolean isAutoSchemaUpdateEnabled();

    @DefaultValue("false")
    @Key("FAIL_ON_UNKNOWN_FIELDS")
    Boolean isFailOnUnknownFields();
}
