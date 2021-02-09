package com.gojek.beast.sink.dlq.gcs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.beast.exception.ErrorWriterFailedException;
import com.gojek.beast.models.Record;
import com.gojek.beast.sink.dlq.RecordsErrorType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class GCSInvalidMessagesWrapper {

    /**
     * Message format for DLQ.
     * {
     *  key: raw_message_key
     *  value: raw_message_value,
     *  error: error_type,
     *  offset: record_offset,
     *  partition: record_partition,
     *  topic: kafka_topic
     * }
     * */

    private final ObjectMapper mapper;
    private final List<Map<String, Object>> invalidMessagesMap;

    public void addInValidMessage(Record record, RecordsErrorType errorType) {
        Map<String, Object> recordValues = new HashMap<>();
        recordValues.put("offset", record.getOffsetInfo().getOffset());
        recordValues.put("partition", record.getOffsetInfo().getPartition());
        recordValues.put("timestamp", record.getOffsetInfo().getTimestamp());
        recordValues.put("error", errorType.toString());
        recordValues.put("topic", record.getOffsetInfo().getTopic());
        if (record.getValue() != null) {
            recordValues.put("value", new String(record.getValue(), StandardCharsets.UTF_8));
        }
        if (record.getKey() != null) {
            recordValues.put("key", new String(record.getKey(), StandardCharsets.UTF_8));
        }
        this.invalidMessagesMap.add(recordValues);
    }

    public byte[] getBytes() {
        StringBuilder fileString = new StringBuilder();
        invalidMessagesMap.forEach(stringObjectMap -> {
            try {
                fileString.append(mapper.writeValueAsString(stringObjectMap));
                fileString.append("\n");
            } catch (JsonProcessingException jpe) {
                log.error("Exception::Failed to parse to JSON: {} records size: {}", jpe.getMessage(), stringObjectMap.size());
                throw new ErrorWriterFailedException(jpe.getMessage(), jpe);
            }
        });
        return fileString.toString().getBytes();
    }
}
