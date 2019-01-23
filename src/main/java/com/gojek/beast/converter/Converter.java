package com.gojek.beast.converter;

import com.gojek.beast.models.Record;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface Converter {
    List<Record> convert(Iterable<ConsumerRecord<byte[], byte[]>> messages) throws InvalidProtocolBufferException;
}
