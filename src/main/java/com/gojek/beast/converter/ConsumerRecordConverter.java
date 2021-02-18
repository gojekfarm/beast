package com.gojek.beast.converter;

import com.gojek.beast.Clock;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.Constants;
import com.gojek.beast.exception.ErrorWriterFailedException;
import com.gojek.beast.exception.NullInputMessageException;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import com.gojek.beast.protomapping.UnknownProtoFields;
import com.gojek.beast.sink.dlq.ErrorWriter;
import com.gojek.beast.sink.dlq.RecordsErrorType;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.parser.Parser;
import com.google.api.client.util.DateTime;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class ConsumerRecordConverter implements Converter {
    private final RowMapper rowMapper;
    private final Parser parser;
    private final Clock clock;
    private final AppConfig appConfig;
    private final Stats statsClient = Stats.client();
    private final ErrorWriter errorWriter;

    public List<Record> convert(final Iterable<ConsumerRecord<byte[], byte[]>> messages) throws InvalidProtocolBufferException {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> message : messages) {
            if (message.value() == null) {
                // don't handle empty message
                if (appConfig.getFailOnNullMessage()) {
                    statsClient.gauge("record.processing.failure,type=null," + statsClient.getBqTags(), 1);
                    throw new NullInputMessageException(message.offset());
                }
                statsClient.increment("kafka.error.records.count,type=null," + statsClient.getBqTags());
                continue;
            }
            OffsetInfo offsetInfo = new OffsetInfo(message.topic(), message.partition(), message.offset(), message.timestamp());
            Map<String, Object> columns = mapToColumns(message);
            if (columns.isEmpty()) {
                invalidRecords.add(new Record(offsetInfo, columns, message.key(), message.value()));
                continue;
            }
            addMetadata(columns, offsetInfo);
            validRecords.add(new Record(offsetInfo, columns, message.key(), message.value()));
        }
        sinkToErrorWriter(invalidRecords);
        return validRecords;
    }

    private Map<String, Object> mapToColumns(ConsumerRecord<byte[], byte[]> message) throws InvalidProtocolBufferException {
        Map<String, Object> columns = Collections.emptyMap();
        try {
            columns = rowMapper.map(parser.parse(message.value()));
        } catch (InvalidProtocolBufferException e) {
            log.info("failed to deserialize message: {} at offset: {}, partition: {}", UnknownProtoFields.toString(message.value()),
                    message.offset(), message.partition());
            if (appConfig.getFailOnDeserializeError()) {
                statsClient.gauge("record.processing.failure,type=deserialize," + statsClient.getBqTags(), 1);
                throw new InvalidProtocolBufferException(e);
            }
        }
        return columns;
    }

    private void addMetadata(Map<String, Object> columns, OffsetInfo offsetInfo) {
        Map<String, Object> offsetMetadata = new HashMap<>();
        offsetMetadata.put(Constants.PARTITION_COLUMN_NAME, offsetInfo.getPartition());
        offsetMetadata.put(Constants.OFFSET_COLUMN_NAME, offsetInfo.getOffset());
        offsetMetadata.put(Constants.TOPIC_COLUMN_NAME, offsetInfo.getTopic());
        offsetMetadata.put(Constants.TIMESTAMP_COLUMN_NAME, new DateTime(offsetInfo.getTimestamp()));
        offsetMetadata.put(Constants.LOAD_TIME_COLUMN_NAME, new DateTime(clock.currentEpochMillis()));

        if (appConfig.getBqMetadataNamespace().isEmpty()) {
            columns.putAll(offsetMetadata);
        } else {
            columns.put(appConfig.getBqMetadataNamespace(), offsetMetadata);
        }
    }

    private void sinkToErrorWriter(List<Record> errorRecordList) {
        if (!errorRecordList.isEmpty()) {
            log.info("Error handler parsed Empty records of size {}, handoff to the writer {}", errorRecordList.size(), errorWriter.getClass().getSimpleName());
            final Status dlqStatus = errorWriter.writeRecords(ImmutableMap.of(RecordsErrorType.DESERIALIZE, errorRecordList));
            if (!dlqStatus.isSuccess()) {
                log.error("Exception::Batch with records size: {} contains DLQ sinkable records but failed to sink", errorRecordList.size());
                throw new ErrorWriterFailedException(dlqStatus.getException().orElse(null));
            }
            statsClient.count("kafka.error.records.count,type=deserialize," + statsClient.getBqTags(), errorRecordList.size());
        }
    }
}
