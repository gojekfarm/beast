package com.gojek.beast.consumer;

import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;
import com.gojek.beast.protomapping.ProtoUpdateListener;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.stats.Stats;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Instant;
import java.util.List;

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

    public void consume() throws WakeupException, InvalidProtocolBufferException {
        if (isClosed()) {
            throw new RuntimeException("Message consumer was closed");
        }
        Instant startTime = Instant.now();
        ConsumerRecords<byte[], byte[]> messages = kafkaConsumer.poll(timeoutMillis);
        statsClient.count("kafka.consumer.poll.messages", messages.count());
        statsClient.timeIt("kafka.consumer.consumption.time", startTime);
        if (messages.isEmpty()) {
            return;
        }
        Instant pollTime = Instant.now();
        log.info("Pulled {} messages", messages.count());
        pushToSink(messages, pollTime);
    }

    private void pushToSink(ConsumerRecords<byte[], byte[]> messages, Instant pollTime) throws InvalidProtocolBufferException {
        List<Record> records;
        final Instant deSerTime = Instant.now();
        ConsumerRecordConverter recordConverter = this.protoUpdateListener.getProtoParser();
        records = recordConverter.convert(messages);
        statsClient.timeIt("kafkaConsumer.batch.deserialization.time", deSerTime);
        sink.push(new Records(records, pollTime));
    }

    public void close() {
        kafkaConsumer.close();
        log.info("Successfully stopped message consumer");
    }

    public boolean isClosed() {
        return kafkaConsumer.isClosed();
    }
}
