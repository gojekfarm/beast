package com.gojek.beast.consumer;

import com.gojek.beast.converter.Converter;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Instant;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class MessageConsumer {

    private final KafkaConsumer kafkaConsumer;
    private final Sink sink;
    private final Converter recordConverter;
    private final long timeoutMillis;

    private final Stats statsClient = Stats.client();

    public Status consume() throws WakeupException {
        Instant startTime = Instant.now();
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeoutMillis);
        statsClient.gauge("kafkaConsumer.poll.messages", messages.count());
        if (messages.isEmpty()) {
            return new SuccessStatus();
        }
        log.info("Pulled {} messages", messages.count());
        Status status = pushToSink(messages);
        statsClient.timeIt("kafkaConsumer.consumption.time", startTime);
        return status;
    }

    private Status pushToSink(ConsumerRecords<byte[], byte[]> messages) {
        List<Record> records;
        try {
            records = recordConverter.convert(messages);
        } catch (ParseException e) {
            Status failure = new FailureStatus(e);
            log.error("Error while converting messages: {}", failure.toString());
            return failure;
        }

        return sink.push(new Records(records));
    }

    public void close() {
        kafkaConsumer.close();
    }
}
