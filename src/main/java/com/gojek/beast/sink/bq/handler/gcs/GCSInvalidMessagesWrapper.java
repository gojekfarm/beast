package com.gojek.beast.sink.bq.handler.gcs;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GCSInvalidMessagesWrapper {

    private final ObjectMapper mapper;
    private final Map<String, Map<String, Object>> invalidMessagesMap; // topic_partition_offset -> message<column, value>

    public void addInValidMessage(String topicOffsetInfo, Map<String, Object> recordValues) {
        this.invalidMessagesMap.put(topicOffsetInfo, recordValues);
    }

    public byte[] getBytes() throws JsonProcessingException {
        return mapper.writeValueAsString(invalidMessagesMap).getBytes();
    }
}
