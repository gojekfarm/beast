package com.gojek.beast.consumer;

import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.protomapping.ProtoUpdateListener;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Instant;
import java.util.List;

import static com.gojek.beast.config.Constants.SUCCESS_STATUS;

@Slf4j
public class MessageConsumer {

    private final KafkaConsumer kafkaConsumer;
    private final Sink sink;
    private final ProtoUpdateListener protoUpdateListener;
    private final long timeoutMillis;
    private final Stats statsClient = Stats.client();

    public MessageConsumer(KafkaConsumer kafkaConsumer, Sink sink, ProtoUpdateListener protoUpdateListener, long timeoutMillis) {
        this.kafkaConsumer = kafkaConsumer;
        this.sink = sink;
        this.protoUpdateListener = protoUpdateListener;
        this.timeoutMillis = timeoutMillis;
    }

    public Status consume() throws WakeupException {
        if (isClosed()) {
            return new FailureStatus(new RuntimeException("Message consumer was closed"));
        }
        Instant startTime = Instant.now();
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeoutMillis);
        statsClient.count("kafka.consumer.poll.messages", messages.count());
        statsClient.timeIt("kafka.consumer.consumption.time", startTime);
        if (messages.isEmpty()) {
            return SUCCESS_STATUS;
        }
        log.info("Pulled {} messages", messages.count());
        return pushToSink(messages);
    }

    private Status pushToSink(ConsumerRecords<byte[], byte[]> messages) {
        Instant pollTime = Instant.now();
        List<Record> records;
        try {
            final Instant deSerTime = Instant.now();
            ConsumerRecordConverter recordConverter = this.protoUpdateListener.getProtoParser();
            records = recordConverter.convert(messages);
            statsClient.timeIt("kafkaConsumer.batch.deserialization.time", deSerTime);
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            Status failure = new FailureStatus(e);
            statsClient.increment("kafka.protobuf.deserialize.errors");
            log.error("Error while converting messages: {}", failure.toString());
            return failure;
        }
        return sink.push(new Records(records, pollTime));
    }

    public void close() {
        kafkaConsumer.close();
        log.info("Successfully stopped message consumer");
    }

    public boolean isClosed() {
        return kafkaConsumer.isClosed();
    }
}
