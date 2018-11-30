package com.gojek.beast.config;

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConfig extends com.gojek.de.Config {
    private final String kafkaPrefix;
    private final String kafkaPrefixRegex;
    private Map<String, Object> consumerConfigs;

    public KafkaConfig(String configPrefix, String... fileConfigPaths) {
        super(fileConfigPaths);
        kafkaPrefix = configPrefix + "_";
        kafkaPrefixRegex = "^" + kafkaPrefix + ".*";
    }

    private Map<String, Object> build() {
        return getMatching(kafkaPrefixRegex)
                .entrySet().stream()
                .collect(Collectors.toMap(e -> removePrefix(e.getKey()), e -> get(e.getKey())));
    }

    public Map<String, Object> get() {
        if (consumerConfigs == null) {
            consumerConfigs = build();
        }
        return consumerConfigs;
    }

    private String removePrefix(String varName) {
        String[] names = varName.replaceAll(kafkaPrefix, "").toLowerCase().split("_");
        return String.join(".", names);
    }
}
