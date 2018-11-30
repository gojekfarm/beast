package com.gojek.beast.consumer;

import com.gojek.beast.converter.Converter;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.bq.Record;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@AllArgsConstructor
public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class.getName());
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final Sink<Record> sink;
    private final Converter recordConverter;
    private final long timeoutMillis;

    public Status consume() {
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeoutMillis);
        LOGGER.info("Pulled {} messages", messages.count());
        if (messages.isEmpty()) {
            return new SuccessStatus();
        }
        List<Record> records;
        try {
            records = recordConverter.convert(messages);
        } catch (ParseException e) {
            Status failure = new FailureStatus(e);
            LOGGER.error("Error while converting messages: {}", failure.toString());
            return failure;
        }
        return sink.push(records);
    }
}
