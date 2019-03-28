package com.gojek.beast.config;

import org.aeonbits.owner.Mutable;

public interface ProtoMappingConfig extends Mutable {
    @Key("PROTO_COLUMN_MAPPING")
    @ConverterClass(ProtoIndexToFieldMapConverter.class)
    ColumnMapping getProtoColumnMapping();

    @DefaultValue("false")
    @Key("ENABLE_AUTO_SCHEMA_UPDATE")
    Boolean isAutoSchemaUpdateEnabled();

    @Key("PROTO_COLUMN_MAPPING_URL")
    String getProtoColumnMappingURL();

    @Key("UPDATE_TABLE_SCHEMA_URL")
    String getUpdateBQTableURL();
}
