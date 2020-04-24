package com.gojek.beast.sink;

import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.MultiException;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static com.gojek.beast.config.Constants.SUCCESS_STATUS;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MultiSinkTest {

    private Sink multiSink;

    @Mock
    private Records records;

    @Test
    public void shouldPushToMultipleSinks() {
        Sink sink1 = mock(Sink.class);
        Sink sink2 = mock(Sink.class);
        Status success = SUCCESS_STATUS;
        when(sink1.push(records)).thenReturn(success);
        when(sink2.push(records)).thenReturn(success);
        multiSink = new MultiSink(Arrays.asList(sink1, sink2));

        Status status = multiSink.push(records);

        verify(sink1).push(records);
        verify(sink2).push(records);
        assertTrue(status.isSuccess());
    }

    @Test
    public void shouldReturnFailureStatusWhenOneSinkFails() {
        Sink sink1 = mock(Sink.class);
        Sink sink2 = mock(Sink.class);
        Status success = SUCCESS_STATUS;
        Exception cause = new Exception();
        Status failure = new FailureStatus(cause);
        when(sink1.push(records)).thenReturn(failure);
        when(sink2.push(records)).thenReturn(success);
        multiSink = new MultiSink(Arrays.asList(sink1, sink2));

        Status status = multiSink.push(records);

        assertFalse(status.isSuccess());
        assertTrue(status.getException().isPresent());
        assertEquals(new MultiException(Collections.singletonList(failure)).toString(), status.getException().get().toString());
    }

    @Test
    public void shouldReturnMultipleExceptionOnSinkFailures() {
        Sink sink1 = mock(Sink.class);
        Sink sink2 = mock(Sink.class);
        Exception cause = new Exception();
        Status failure = new FailureStatus(cause);
        when(sink1.push(records)).thenReturn(failure);
        when(sink2.push(records)).thenReturn(failure);
        multiSink = new MultiSink(Arrays.asList(sink1, sink2));

        Status status = multiSink.push(records);

        assertFalse(status.isSuccess());
        MultiException causes = (MultiException) status.getException().get();
        assertTrue(causes.getException().isPresent());
        assertEquals(new MultiException(Arrays.asList(failure, failure)).toString(), causes.toString());
    }
}
