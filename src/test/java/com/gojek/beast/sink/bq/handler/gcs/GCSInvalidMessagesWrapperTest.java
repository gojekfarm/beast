package com.gojek.beast.sink.bq.handler.gcs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class GCSInvalidMessagesWrapperTest {

    @Test
    public void testMessagesCouldBeAdded() throws IOException {
        GCSInvalidMessagesWrapper msgWrapper = new GCSInvalidMessagesWrapper(new ObjectMapper(), new HashMap<>());
        Map<String, Object> columns = new HashMap<>();
        columns.put("column1", "value1");
        msgWrapper.addInValidMessage("test-topic-p1-offest1", columns);
        assertTrue(msgWrapper.getBytes().length > 0);
        TypeReference<Map<String, Map<String, String>>> deSerObj = new TypeReference<Map<String, Map<String, String>>>() { };
        Map<String, Map<String, String>> readObj =  new ObjectMapper().readValue(msgWrapper.getBytes(), deSerObj);
        Map<String, String> colmns = readObj.get("test-topic-p1-offest1");
        assertTrue(colmns.get("column1").equals("value1"));
    }
}
