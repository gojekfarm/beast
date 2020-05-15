package com.gojek.beast.config;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapPropertyConverter implements Converter<Map<String, String>> {
    private static final String VALUE_SEPARATOR = "=";
    static final String ELEMENT_SEPARATOR = ",";

    public Map<String, String> convert(Method method, String input) {
        Map<String, String> result = new LinkedHashMap<>();
        String[] chunks = input.split(ELEMENT_SEPARATOR, -1);
        for (String chunk : chunks) {
            String[] entry = chunk.split(VALUE_SEPARATOR, -1);
            String key = entry[0].trim();
            String value = entry[1].trim();
            result.put(key, value);
        }
        return result;
    }
}
