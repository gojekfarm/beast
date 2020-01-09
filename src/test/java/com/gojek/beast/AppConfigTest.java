package com.gojek.beast;

import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.BQConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Ignore
public class AppConfigTest {

    private AppConfig config;
    private BQConfig bqConfig;

    @Before
    public void setUp() throws Exception {
        config = ConfigFactory.create(AppConfig.class, System.getenv());
        bqConfig = ConfigFactory.create(BQConfig.class, System.getenv());
    }

    @Test
    public void shouldReadApplicationConfiguration() {
        assertEquals("test-table", bqConfig.getTable());
        assertEquals("test-dataset", bqConfig.getDataset());
        assertEquals("true", config.isGCSErrorSinkEnabled());
        assertEquals("beast/test", config.getGcsPathPrefix());
    }
}
