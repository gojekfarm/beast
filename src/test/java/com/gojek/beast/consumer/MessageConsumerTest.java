package com.gojek.beast.consumer;

import com.gojek.beast.parser.ConsumerRecordParser;
import com.gojek.beast.sink.Sink;
import com.gojek.beast.sink.Status;
import com.gojek.beast.sink.bq.Record;
import com.gojek.beast.sink.bq.SuccessStatus;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageConsumerTest {
    @Mock
    private ConsumerRecords messages;
    private long timeout = 10;
    @Mock
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    @Mock
    private Sink<Record> sink;
    @Mock
    private ConsumerRecordParser recordParser;
    @Mock
    private List<Record> records;

    private MessageConsumer consumer;
    private Status success = new SuccessStatus();

    @Before
    public void setUp() {
        consumer = new MessageConsumer(kafkaConsumer, sink, recordParser, timeout);
        when(kafkaConsumer.poll(timeout)).thenReturn(messages);
    }

    @Test
    public void shouldConsumeMessagesAndPushToSink() {
        when(recordParser.getRecords(messages)).thenReturn(records);
        when(sink.push(records)).thenReturn(success);
        InOrder callOrder = inOrder(recordParser, sink);

        Status status = consumer.consume();

        callOrder.verify(recordParser).getRecords(messages);
        callOrder.verify(sink).push(records);
        assertTrue(status.isSuccess());
    }
}
