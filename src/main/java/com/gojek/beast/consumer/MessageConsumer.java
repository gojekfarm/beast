package com.gojek.beast.consumer;

import com.gojek.beast.converter.Converter;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

@AllArgsConstructor
public class MessageConsumer {

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final Sink<Record> sink;
    private final Converter recordConverter;
    private final long timeout;

    public Status consume() {
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeout);
        List<Record> records;
        try {
            records = recordConverter.convert(messages);
        } catch (ParseException e) {
            return new FailureStatus(e);
        }
        return sink.push(records);
    }
}
