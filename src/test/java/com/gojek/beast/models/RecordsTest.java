package com.gojek.beast.models;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.gradle.internal.impldep.org.testng.AssertJUnit.assertSame;
import static org.junit.Assert.assertEquals;

public class RecordsTest {

    @Test
    public void shouldGetMaxOffsetFromRecordsForAPartition() {
        String topic = "default-topic";
        int maxOffset = 102;
        int partition = 0;
        List<Record> records = Arrays.asList(
                new Record(new OffsetInfo(topic, partition, 100), null),
                new Record(new OffsetInfo(topic, partition, maxOffset), null),
                new Record(new OffsetInfo(topic, partition, 101), null)
        );

        Map<TopicPartition, OffsetAndMetadata> actualMaxOffsetInfo = new Records(records).getPartitionsMaxOffset();

        assertEquals(1, actualMaxOffsetInfo.size());
        assertEquals(maxOffset, actualMaxOffsetInfo.get(new TopicPartition(topic, partition)).offset());
    }

    @Test
    public void shouldGetMaxOffsetFromRecordsForMultiplePartitions() {
        String topic = "default-topic";
        int partition0MaxOffset = 102;
        int partition1MaxOffset = 105;
        int partition0 = 0;
        int partition1 = 1;
        List<Record> records = Arrays.asList(
                new Record(new OffsetInfo(topic, partition0, 100), null),
                new Record(new OffsetInfo(topic, partition0, partition0MaxOffset), null),
                new Record(new OffsetInfo(topic, partition0, 101), null),
                new Record(new OffsetInfo(topic, partition1, partition1MaxOffset), null),
                new Record(new OffsetInfo(topic, partition1, 101), null),
                new Record(new OffsetInfo(topic, partition1, 102), null)
        );

        Map<TopicPartition, OffsetAndMetadata> actualMaxOffsetInfo = new Records(records).getPartitionsMaxOffset();

        assertEquals(2, actualMaxOffsetInfo.size());
        assertEquals(partition0MaxOffset, actualMaxOffsetInfo.get(new TopicPartition(topic, partition0)).offset());
        assertEquals(partition1MaxOffset, actualMaxOffsetInfo.get(new TopicPartition(topic, partition1)).offset());
    }

    @Test
    public void shouldCacheTheMaxOffsetInfo() {
        Records records = new Records(Arrays.asList(new Record(new OffsetInfo("topic", 0, 100), null)));

        Map<TopicPartition, OffsetAndMetadata> actualMaxOffsetInfo = records.getPartitionsMaxOffset();

        assertSame(actualMaxOffsetInfo, records.getPartitionsMaxOffset());
        assertSame(actualMaxOffsetInfo, records.getPartitionsMaxOffset());
    }
}
