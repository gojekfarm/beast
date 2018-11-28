package com.gojek.beast.parser;

import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class ConsumerRecordParser {
    private final MessageTransformer transformer;
    private final Parser parser;

    public List<Record> getRecords(final Iterable<ConsumerRecord<byte[], byte[]>> messages) {
        ArrayList<Record> records = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> message : messages) {
            byte[] value = message.value();
            Map<String, Object> columns = transformer.getFields(parser.parse(value));
            records.add(new Record(columns));
        }
        return records;
    }

}
