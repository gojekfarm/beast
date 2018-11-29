package com.gojek.beast.converter;

import com.gojek.beast.models.ParseException;
import com.gojek.beast.parser.Parser;
import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class ConsumerRecordConverter implements Converter {
    private final RowMapper rowMapper;
    private final Parser parser;

    public List<Record> convert(final Iterable<ConsumerRecord<byte[], byte[]>> messages) throws ParseException {
        ArrayList<Record> records = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> message : messages) {
            byte[] value = message.value();
            Map<String, Object> columns = rowMapper.map(parser.parse(value));
            records.add(new Record(columns));
        }
        return records;
    }

}
