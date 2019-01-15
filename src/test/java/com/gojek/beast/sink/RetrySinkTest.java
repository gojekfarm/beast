package com.gojek.beast.sink;

import com.gojek.beast.backoff.BackOff;
import com.gojek.beast.backoff.ExponentialBackOffProvider;
import com.gojek.beast.models.FailureStatus;
import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.models.SuccessStatus;
import com.google.cloud.bigquery.BigQueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static junit.framework.TestCase.assertTrue;
import static org.gradle.internal.impldep.org.testng.AssertJUnit.assertFalse;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RetrySinkTest {
    private RetrySink retrySink;
    @Mock
    private Records records;
    @Mock
    private Sink failureSink;
    @Mock
    private Sink successSink;

    private ExponentialBackOffProvider backOffProvider;
    private SuccessStatus successStatus = new SuccessStatus();
    private FailureStatus failureStatus = new FailureStatus(new Exception());
    private BigQueryException bigQueryException = new BigQueryException(10, "Some Error");


    @Test
    public void shouldPushToSinkUntilMaxAttempts() {
        int maxPushAttempts = 5;
        backOffProvider = new ExponentialBackOffProvider(10, 10000, 2, new BackOff());
        when(failureSink.push(records)).thenReturn(failureStatus);
        retrySink = new RetrySink(failureSink, backOffProvider, maxPushAttempts);

        Status status = retrySink.push(records);

        assertFalse(status.isSuccess());
        verify(failureSink, times(maxPushAttempts)).push(records);
    }

    @Test
    public void shouldNotRetryAfterSuccess() {
        int maxPushAttempts = 5;
        backOffProvider = new ExponentialBackOffProvider(10, 1000, 2, new BackOff());
        when(failureSink.push(records)).thenReturn(failureStatus, failureStatus, failureStatus, successStatus, failureStatus);
        retrySink = new RetrySink(failureSink, backOffProvider, maxPushAttempts);

        Status status = retrySink.push(records);

        assertTrue(status.isSuccess());
        verify(failureSink, times(4)).push(records);
    }

    @Test
    public void shouldNotRetryIfSuccess() {
        int maxPushAttempts = 5;
        backOffProvider = new ExponentialBackOffProvider(10, 1000, 2, new BackOff());
        when(successSink.push(records)).thenReturn(new SuccessStatus());
        retrySink = new RetrySink(successSink, backOffProvider, maxPushAttempts);

        Status status = retrySink.push(records);

        assertTrue(status.isSuccess());
        verify(successSink, times(1)).push(records);
    }

    @Test
    public void shouldRetryForMaxAttemptsIfExceptionIsThrown() {
        int maxPushAttempts = 5;
        backOffProvider = new ExponentialBackOffProvider(10, 1000, 2, new BackOff());
        when(failureSink.push(records)).thenThrow(new BigQueryException(10, "Some Error"));
        retrySink = new RetrySink(failureSink, backOffProvider, maxPushAttempts);

        Status status = retrySink.push(records);
        assertFalse(status.isSuccess());
        verify(failureSink, times(maxPushAttempts)).push(records);
    }

    @Test
    public void shouldRetryUntilSuccessIfExceptionIsThrown() {
        int maxPushAttempts = 5;
        backOffProvider = new ExponentialBackOffProvider(10, 1000, 2, new BackOff());
        when(failureSink.push(records)).thenThrow(bigQueryException, bigQueryException, bigQueryException).thenReturn(successStatus);

        retrySink = new RetrySink(failureSink, backOffProvider, maxPushAttempts);

        Status status = retrySink.push(records);
        assertTrue(status.isSuccess());
        verify(failureSink, times(4)).push(records);
    }
}
