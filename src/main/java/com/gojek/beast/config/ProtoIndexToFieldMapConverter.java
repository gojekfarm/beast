package com.gojek.beast.config;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public class ProtoIndexToFieldMapConverter implements org.aeonbits.owner.Converter<ColumnMapping> {
    @Override
    public ColumnMapping convert(Method method, String input) {
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> m = new Gson().fromJson(input, type);
        ColumnMapping properties = getProperties(m);
        if (Boolean.valueOf(System.getenv(Constants.Config.COLUMN_MAPPING_CHECK_DUPLICATES))) {
            validate(properties);
        }
        return properties;
    }

    private ColumnMapping getProperties(Map<String, Object> inputMap) {
        ColumnMapping properties = new ColumnMapping();
        for (String key : inputMap.keySet()) {
            Object value = inputMap.get(key);
            if (value instanceof String) {
                properties.put(key, value);
            } else if (value instanceof Map) {
                ColumnMapping properties1 = getProperties((Map) value);
                properties.put(key, properties1);
            }
        }
        return properties;
    }

    private void validate(ColumnMapping properties) {
        DuplicateFinder duplicateFinder = flattenValues(properties)
                .collect(DuplicateFinder::new, DuplicateFinder::accept, DuplicateFinder::combine);
        if (duplicateFinder.duplicates.size() > 0) {
            throw new IllegalArgumentException("duplicates found in PROTO_TO_COLUMN_MAPPING for : " + duplicateFinder.duplicates);
        }
    }

    private Stream<String> flattenValues(Properties properties) {
        return properties
                .entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .flatMap(v -> {
                    if (v instanceof String) {
                        return Stream.of((String) v);
                    } else if (v instanceof Properties) {
                        return flattenValues((Properties) v);
                    } else {
                        return Stream.empty();
                    }
                });
    }

    private class DuplicateFinder implements Consumer<String> {
        private Set<String> processedValues = new HashSet<>();
        private List<String> duplicates = new ArrayList<>();

        @Override
        public void accept(String o) {
            if (processedValues.contains(o)) {
                duplicates.add(o);
            } else {
                processedValues.add(o);
            }
        }

        void combine(DuplicateFinder other) {
            other.processedValues
                    .forEach(v -> {
                        if (processedValues.contains(v)) {
                            duplicates.add(v);
                        } else {
                            processedValues.add(v);
                        }
                    });
        }
    }
}
