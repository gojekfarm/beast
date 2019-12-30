package com.gojek.beast.stats;

import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class StatsTest {

    @Test
    public void testEnvironmentTagsAreParsedAsList() {
        Stats client = Stats.client();
        final List<String> nullTags = client.getStatsdDefaultTags(null);
        assertTrue(nullTags.size() == 0);
        assertTrue(client.getStatsdDefaultTags("").size() == 0);
        assertTrue(client.getStatsdDefaultTags("var1=val1").size() == 1);
        assertTrue(client.getStatsdDefaultTags("var1=val1").get(0).split("=").length == 2);
        assertTrue(client.getStatsdDefaultTags("var1,val1").size() == 0);
        List<String> tagList = client.getStatsdDefaultTags("var1=val1,var2=val2");
        assertTrue(tagList.size() == 2);
        assertTrue(tagList.get(0).split("=").length == 2);
        assertTrue(tagList.get(1).split("=").length == 2);
    }
}
