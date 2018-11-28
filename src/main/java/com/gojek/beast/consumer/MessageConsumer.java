package com.gojek.beast.consumer;

import com.gojek.beast.parser.ConsumerRecordParser;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.Status;
import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;

@AllArgsConstructor
public class MessageConsumer {

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final Sink<Record> sink;
    private final ConsumerRecordParser recordParser;
    private final long timeout;

    public Status consume() {
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeout);
        List<Record> records = recordParser.getRecords(messages);
        return sink.push(records);
    }
}
