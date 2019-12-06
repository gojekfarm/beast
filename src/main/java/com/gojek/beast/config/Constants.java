package com.gojek.beast.config;

public class Constants {
    public static final String PARTITION_COLUMN_NAME = "message_partition";
    public static final String OFFSET_COLUMN_NAME = "message_offset";
    public static final String TOPIC_COLUMN_NAME = "message_topic";
    public static final String TIMESTAMP_COLUMN_NAME = "message_timestamp";
    public static final String LOAD_TIME_COLUMN_NAME = "load_time";
    public static final String DATE_PREFIX = "dt=";
    public static final String DATE_PATTERN = "yyyy-MM-dd";

    public class Config {
        public static final String COLUMN_MAPPING_CHECK_DUPLICATES = "COLUMN_MAPPING_CHECK_DUPLICATES";
        public static final String RECORD_NAME = "record_name";
    }
}
