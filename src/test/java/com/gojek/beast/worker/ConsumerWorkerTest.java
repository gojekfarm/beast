package com.gojek.beast.worker;

import com.gojek.beast.consumer.MessageConsumer;
import com.gojek.beast.models.SuccessStatus;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerWorkerTest {

    @Mock
    private MessageConsumer consumer;

    @Test
    public void shouldConsumeMessagesWhenNotStopped() throws InterruptedException {
        Worker worker = new ConsumerWorker("consumer", consumer, new WorkerState());
        when(consumer.consume()).thenReturn(new SuccessStatus());
        new Thread(worker).start();

        Thread.sleep(50L);
        worker.stop("some reason");
        verify(consumer, atLeast(5)).consume();
    }

    @Test
    public void shouldConsumeOnlyOnceWhenStopped() throws InterruptedException {
        Worker worker = new ConsumerWorker("consumer", consumer, new WorkerState());
        worker.stop("some reason");

        new Thread(worker).start();
        Thread.sleep(10L);

        verify(consumer, times(1)).consume();

    }

    @Test
    public void shouldStopConsumptionWhenWakeupExceptionIsThrown() throws InterruptedException {
        Worker worker = new ConsumerWorker("consumer", consumer, new WorkerState());
        doThrow(new WakeupException()).when(consumer).consume();

        new Thread(worker).start();

        Thread.sleep(100);
        verify(consumer).consume();
        verify(consumer).close();
    }
}
