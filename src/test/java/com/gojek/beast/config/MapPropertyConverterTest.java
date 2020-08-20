package com.gojek.beast.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapPropertyConverterTest {

    @Test
    public void shouldhandleNonEmptyProperty() throws Exception {
        String attributes = "label1=val1";
        Map<String, String> props = new MapPropertyConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("label1", "val1");

        assertEquals(expectedProperties, props);
    }

    @Test
    public void shouldhandleEmptyProperty() throws Exception {
        String attributes = "label1=";
        Map<String, String> props = new MapPropertyConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("label1", "");

        assertEquals(expectedProperties, props);
    }

    @Test
    public void shouldhandleEmptyRequest() throws Exception {
        String attributes = "";
        Map<String, String> props = new MapPropertyConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        assertEquals(expectedProperties, props);
    }
}
