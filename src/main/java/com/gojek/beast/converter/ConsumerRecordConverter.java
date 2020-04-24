package com.gojek.beast.converter;

import com.gojek.beast.Clock;
import com.gojek.beast.config.Constants;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.de.stencil.parser.Parser;
import com.google.api.client.util.DateTime;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class ConsumerRecordConverter implements Converter {
    private final RowMapper rowMapper;
    private final Parser parser;
    private final Clock clock;

    public List<Record> convert(final Iterable<ConsumerRecord<byte[], byte[]>> messages) throws InvalidProtocolBufferException {
        ArrayList<Record> records = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> message : messages) {
            Map<String, Object> columns = rowMapper.map(parser.parse(message.value()));
            OffsetInfo offsetInfo = new OffsetInfo(message.topic(), message.partition(), message.offset(), message.timestamp());
            addMetadata(columns, offsetInfo);
            records.add(new Record(offsetInfo, columns));
        }
        return records;
    }

    private void addMetadata(Map<String, Object> columns, OffsetInfo offsetInfo) {
        columns.put(Constants.PARTITION_COLUMN_NAME, offsetInfo.getPartition());
        columns.put(Constants.OFFSET_COLUMN_NAME, offsetInfo.getOffset());
        columns.put(Constants.TOPIC_COLUMN_NAME, offsetInfo.getTopic());
        columns.put(Constants.TIMESTAMP_COLUMN_NAME, new DateTime(offsetInfo.getTimestamp()));
        columns.put(Constants.LOAD_TIME_COLUMN_NAME, new DateTime(clock.currentEpochMillis()));
    }
}
