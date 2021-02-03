package com.gojek.beast.sink.dlq.gcs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.beast.config.Constants;
import com.gojek.beast.exception.ErrorWriterFailedException;
import com.gojek.beast.models.Record;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.dlq.ErrorWriter;
import com.gojek.beast.sink.dlq.RecordsErrorType;
import com.gojek.beast.sink.dlq.WriteStatus;
import com.gojek.beast.stats.Stats;
import com.gojek.beast.util.ApplicationUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@AllArgsConstructor
@Slf4j
public class GCSErrorWriter implements ErrorWriter {

    private final Storage gcsStore;
    private final String gcsBucket; // <bucket> to store
    private final String gcsBasePathPrefix; // path prefix
    private final Stats statsClient = Stats.client();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Status writeRecords(Map<RecordsErrorType, List<Record>> errorRecords) {
        if (errorRecords.isEmpty()) {
            return new WriteStatus(true, Optional.ofNullable(null));
        }
        final Instant startTime = Instant.now();
        try {
            storeMessagesInGCS(errorRecords);
        } catch (StorageException se) {
            log.error("Exception::Failed to write to GCS: {} records size: {}", se, errorRecords.size());
            throw new ErrorWriterFailedException(se.getMessage(), se);
        }
        statsClient.timeIt("sink.gcs.push.invalid.time", startTime);
        //alter the insert status - as successful
        return new WriteStatus(true, Optional.ofNullable(null));
    }

    private void storeMessagesInGCS(final Map<RecordsErrorType, List<Record>> errorRecords) throws StorageException {
        //get all messages to serialize per topic
        final Map<String, GCSInvalidMessagesWrapper> topicMessagesMap = getMessagesToSerializePerTopic(errorRecords);
        //serialize the messages in GCS for each topic - a file with all messages per topic is stored in GCS
        topicMessagesMap.keySet().forEach(topicName -> {
            final String fileName = UUID.randomUUID().toString();
            final String pathPrefix = gcsBasePathPrefix + "/" + topicName + "/" + getFormattedDatePrefix(Instant.now()) + "/";
            final BlobId blobId = BlobId.of(gcsBucket, pathPrefix + fileName);
            final Map<String, String> metaDataMap = new HashMap<>();
            metaDataMap.put("topic", topicName);
            metaDataMap.put("uuid", fileName);
            final BlobInfo objectInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").setMetadata(metaDataMap).build();
            final Blob objectCreated = gcsStore.create(objectInfo, topicMessagesMap.get(topicName).getBytes());
        });
        log.info("Pushing {} records to GCS success?: {}", errorRecords.size(), true);
    }

    private String getFormattedDatePrefix(Instant date) {
        //results in date like dt=2019-03-31
        return Constants.DATE_PREFIX + ApplicationUtil.getFormattedDate(Constants.DATE_PATTERN, date);
    }

    private Map<String, GCSInvalidMessagesWrapper> getMessagesToSerializePerTopic(final Map<RecordsErrorType, List<Record>> errorRecords) {
        //create records for each topic
        final Map<String, GCSInvalidMessagesWrapper> topicMessagesMap = new HashMap<>();
        errorRecords.forEach((recordsErrorType, records) -> {
            records.forEach(record -> {
                final String topicName = record.getOffsetInfo().getTopic();
                final GCSInvalidMessagesWrapper messageWrapper = topicMessagesMap.getOrDefault(topicName,
                        new GCSInvalidMessagesWrapper(mapper, new ArrayList<>()));
                messageWrapper.addInValidMessage(record, recordsErrorType);
                topicMessagesMap.put(topicName, messageWrapper); // add this wrapper for each topic
            });
        });
        return topicMessagesMap;
    }
}
