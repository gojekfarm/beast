package com.gojek.beast.config;

import org.aeonbits.owner.Config;

public interface AppConfig extends Config {
    @Key("CONSUMER_POLL_TIMEOUT_MS")
    @DefaultValue("9223372036854775807")
    Long getConsumerPollTimeoutMs();

    @Key("READ_QUEUE_CAPACITY")
    @DefaultValue("20")
    Integer getReadQueueCapacity();

    @Key("COMMIT_QUEUE_CAPACITY")
    @DefaultValue("200")
    Integer getCommitQueueCapacity();

    @Key("BQ_WORKER_POLL_TIMEOUT_MS")
    @DefaultValue("50")
    Long getBqWorkerPollTimeoutMs();

    @Key("BQ_WORKER_POOL_SIZE")
    @DefaultValue("5")
    Integer getBqWorkerPoolSize();

    @Key("KAFKA_CONSUMER_CONFIG_PREFIX")
    @DefaultValue("KAFKA_CONSUMER")
    String getKafkaConfigPrefix();

    @Key("KAFKA_TOPIC")
    String getKafkaTopic();

    @Key("STATSD_HOST")
    String getStatsdHost();

    @Key("STATSD_PORT")
    Integer getStatsdPort();

    @Key("STATSD_PREFIX")
    String getStatsdPrefix();

    @DefaultValue("false")
    @Key("STATSD_ENABLED")
    Boolean isStatsdEnabled();

    @Key("STATSD_TAGS")
    @DefaultValue("")
    String getStatsdTags();

    @DefaultValue("15000")
    @Key("OFFSET_ACK_TIMEOUT")
    long getOffsetAckTimeoutMs();

    @DefaultValue("true")
    @Key(Constants.Config.COLUMN_MAPPING_CHECK_DUPLICATES)
    Boolean isColumnMappingDuplicateValidationEnabled();

    @DefaultValue("false")
    @Key("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT")
    Boolean isAutoCommitEnabled();

    @DefaultValue("5")
    @Key("MAX_BQ_PUSH_ATTEMPTS")
    Integer getMaxPushAttempts();

    @DefaultValue("false")
    @Key("ENABLE_GCS_ERROR_SINK")
    Boolean isGCSErrorSinkEnabled();

    @Key("GCS_BUCKET")
    String getGcsBucket();

    @Key("GCS_PATH_PREFIX")
    String getGcsPathPrefix();

    @Key("GCS_WRITER_PROJECT_NAME")
    String getGcsWriterProject();

    @DefaultValue("2000")
    @Key("OFFSET_COMMIT_TIME")
    long getOffsetCommitTime();
}
