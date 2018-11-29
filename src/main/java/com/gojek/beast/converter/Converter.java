package com.gojek.beast.converter;

import com.gojek.beast.models.ParseException;
import com.gojek.beast.sink.bq.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface Converter {
    List<Record> convert(Iterable<ConsumerRecord<byte[], byte[]>> messages) throws ParseException;
}
