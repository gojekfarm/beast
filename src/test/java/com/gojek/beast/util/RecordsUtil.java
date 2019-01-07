package com.gojek.beast.util;

import com.gojek.beast.Clock;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Records;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordsUtil {

    private final int defaultAge = 24;
    private int offsetCount = 0;

    public Records createRecords(String namePrefix, int total) {
        List<Record> users = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            long now = new Clock().currentEpochMillis();
            OffsetInfo offsetInfo = new OffsetInfo("topic" + namePrefix, 0, offsetCount++, now);
            users.add(new Record(offsetInfo, createUser(namePrefix + total)));
        }
        return new Records(users);
    }

    public Map<String, Object> createUser(String name) {
        HashMap<String, Object> user = new HashMap<>();
        user.put("name", name);
        user.put("age", defaultAge);
        return user;
    }
}
