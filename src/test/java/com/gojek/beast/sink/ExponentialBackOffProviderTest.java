package com.gojek.beast.sink;

import com.gojek.beast.backoff.BackOff;
import com.gojek.beast.backoff.ExponentialBackOffProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ExponentialBackOffProviderTest {
    @Mock
    private BackOff backOff;

    private final int initialExpiryTimeInMS = 10;

    private final int maximumBackoffTimeInMS = 1000 * 60;

    private final int backOffRate = 2;

    private ExponentialBackOffProvider exponentialBackOffProvider;


    @Before
    public void setup() {
        exponentialBackOffProvider = new ExponentialBackOffProvider(initialExpiryTimeInMS, maximumBackoffTimeInMS, backOffRate, backOff);
    }

    @Test
    public void shouldBeWithinMaxBackoffTime() {

        exponentialBackOffProvider.backOff(100000000);
        verify(backOff).inMilliSeconds(maximumBackoffTimeInMS);
    }

    @Test
    public void shouldBackoffExponentially() {

        exponentialBackOffProvider.backOff(1);
        verify(backOff).inMilliSeconds(20);

        exponentialBackOffProvider.backOff(4);
        verify(backOff).inMilliSeconds(160);
    }

    @Test
    public void shouldSleepForBackOffTimeOnFirstRetry() throws Exception {
        exponentialBackOffProvider.backOff(0);

        verify(backOff).inMilliSeconds(initialExpiryTimeInMS);
    }
}
