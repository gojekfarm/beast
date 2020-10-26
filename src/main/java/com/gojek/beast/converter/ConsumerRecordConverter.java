package com.gojek.beast.converter;

import com.gojek.beast.Clock;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.Constants;
import com.gojek.beast.exception.NullInputMessageException;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.parser.Parser;
import com.google.api.client.util.DateTime;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@AllArgsConstructor
public class ConsumerRecordConverter implements Converter {
    private final RowMapper rowMapper;
    private final Parser parser;
    private final Clock clock;
    private final AppConfig appConfig;
    private final Stats statsClient = Stats.client();

    public List<Record> convert(final Iterable<ConsumerRecord<byte[], byte[]>> messages) throws InvalidProtocolBufferException {
        ArrayList<Record> records = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> message : messages) {
            Map<String, Object> columns;
            OffsetInfo offsetInfo = new OffsetInfo(message.topic(), message.partition(), message.offset(), message.timestamp());
            if (message.value() != null) {
                columns = rowMapper.map(parser.parse(message.value()));
                addMetadata(columns, offsetInfo);
            } else {
                columns = Collections.emptyMap();
                statsClient.increment("kafka.batch.records.null," + statsClient.getBqTags());
                if (appConfig.getFailOnNullMessage()) {
                    throw new NullInputMessageException(message.offset());
                }
            }
            records.add(new Record(offsetInfo, columns));
        }
        return records;
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
}
