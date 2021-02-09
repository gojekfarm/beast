package com.gojek.beast.sink.dlq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.beast.models.OffsetInfo;
import com.gojek.beast.models.Record;
import com.gojek.beast.sink.dlq.gcs.GCSInvalidMessagesWrapper;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class GCSInvalidMessagesWrapperTest {

    @Test
    public void testMessagesCouldBeAdded() throws IOException {
        // encode message
        GCSInvalidMessagesWrapper msgWrapper = new GCSInvalidMessagesWrapper(new ObjectMapper(), new ArrayList<>());
        Map<String, Object> columns = new HashMap<>();
        columns.put("column1", "value1");
        OffsetInfo oi = new OffsetInfo("test-topic-p1", 0, 10, 1000000);
        Record record = new Record(oi, columns, "key".getBytes(), "val".getBytes());
        msgWrapper.addInValidMessage(record, RecordsErrorType.DESERIALIZE);

        assertTrue(msgWrapper.getBytes().length > 0);

        // decode message
        List<Map<String, String>> records = Arrays.stream(new String(msgWrapper.getBytes()).split("\n")).map(msg -> {
            TypeReference<Map<String, String>> deSerObj = new TypeReference<Map<String, String>>() { };
            Map<String, String> readObj = Collections.emptyMap();
            try {
                readObj =  new ObjectMapper().readValue(msg.getBytes(), deSerObj);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return readObj;
        }).collect(Collectors.toList());

        Map<String, String> elems = records.get(0);
        assertTrue(elems.get("offset").equals("10"));
        assertTrue(elems.get("key").equals("key"));
        assertTrue(elems.get("value").equals("val"));
        assertTrue(elems.get("error").equals(RecordsErrorType.DESERIALIZE.toString()));
    }
}
