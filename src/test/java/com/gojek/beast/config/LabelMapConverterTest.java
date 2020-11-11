package com.gojek.beast.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LabelMapConverterTest {

    @Test
    public void shouldHandleNonEmptyProperty() throws Exception {
        String attributes = "label1=val1";
        Map<String, String> props = new LabelMapConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("label1", "val1");

        assertEquals(expectedProperties, props);
    }

    @Test
    public void shouldTrimLongLabelProperty() throws Exception {
        String attributes = "label1=val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1";
        Map<String, String> props = new LabelMapConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("label1", "val1val1val1val1val1val1val1val1val1val1val1val1val1val1val1val");

        assertEquals(expectedProperties, props);
    }

    @Test
    public void shouldHandleEmptyLabelValueProperty() throws Exception {
        String attributes = "label1=";
        Map<String, String> props = new LabelMapConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("label1", "");

        assertEquals(expectedProperties, props);
    }

    @Test
    public void shouldhandleEmptyRequest() throws Exception {
        String attributes = "";
        Map<String, String> props = new LabelMapConverter().convert(null, attributes);

        Map<String, String> expectedProperties = new HashMap<>();
        assertEquals(expectedProperties, props);
    }
}
