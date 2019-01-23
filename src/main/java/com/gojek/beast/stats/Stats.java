package com.gojek.beast.stats;

import com.gojek.beast.config.AppConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public final class Stats {
    private static final Stats STATS_CLIENT = new Stats();

    private StatsDClient statsDClient;
    private AppConfig appConfig;
    private String defaultTags;

    private Stats() {
        this.appConfig = ConfigFactory.create(AppConfig.class, System.getenv());
        this.statsDClient = appConfig.isStatsdEnabled()
                ? new NonBlockingStatsDClient(appConfig.getStatsdPrefix(), appConfig.getStatsdHost(), appConfig.getStatsdPort())
                : new NoOpStatsDClient();
        defaultTags = getDefaultTags();
    }

    public StatsDClient getStatsDClient() {
        return statsDClient;
    }

    public static Stats client() {
        return STATS_CLIENT;
    }

    public void count(String metric, long delta) {
        this.statsDClient.count(metric + defaultTags, delta);
    }

    public void increment(String metric) {
        this.statsDClient.increment(metric + getDefaultTags());
    }

    public void gauge(String metric, long delta) {
        this.statsDClient.gauge(metric + defaultTags, delta);
    }

    private String getDefaultTags() {
        HashMap<String, String> desiredTags = new HashMap<>();
        desiredTags.put("NODE_NAME", "node");
        desiredTags.put("POD_NAME", "pod");
        desiredTags.put("KAFKA_CONSUMER_GROUP_ID", "consumer");

        List<String> tags = desiredTags.entrySet().stream().map((entry) -> {
            String envVar = System.getenv(entry.getKey());
            if (envVar != null) {
                return String.format("%s=%s", entry.getValue(), envVar);
            }
            return "";
        }).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        return "," + StringUtils.join(tags, ",");
    }

    public void timeIt(String metric, Instant start) {
        Instant end = Instant.now();
        long latencyMillis = end.toEpochMilli() - start.toEpochMilli();
        statsDClient.recordExecutionTime(metric + defaultTags, latencyMillis);
    }
}
