package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerWorkerTest {

    @Mock
    private MessageConsumer consumer;

    @Test
    public void shouldConsumeMessagesWhenNotStopped() throws InterruptedException {
        Worker worker = new ConsumerWorker(consumer);

        new Thread(worker).start();

        Thread.sleep(50L);
        worker.stop();
        verify(consumer, atLeast(5)).consume();
    }

    @Test
    public void shouldConsumeOnlyOnceWhenStopped() throws InterruptedException {
        Worker worker = new ConsumerWorker(consumer);
        worker.stop();

        new Thread(worker).start();
        Thread.sleep(10L);

        verify(consumer, times(1)).consume();

    }
}
