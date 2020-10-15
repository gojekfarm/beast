package com.gojek.beast.sink.bq;

import com.gojek.beast.Clock;
import com.gojek.beast.TestMessage;
import com.gojek.beast.config.AppConfig;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.converter.ConsumerRecordConverter;
import com.gojek.beast.converter.RowMapper;
import com.gojek.beast.models.Record;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.*;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BaseBQTest {

    private GoogleCredentials getGoogleCredentials() {
        GoogleCredentials credentials = null;
        File credentialsPath = new File(System.getenv("GOOGLE_CREDENTIALS"));
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return credentials;
    }

    protected BigQuery authenticatedBQ() {
        return BigQueryOptions.newBuilder()
                .setCredentials(getGoogleCredentials())
                .build().getService();
    }

    protected MockHttpTransport getTransporter(final MockLowLevelHttpResponse httpResponse) {
        return new MockHttpTransport.Builder()
                .setLowLevelHttpRequest(
                        new MockLowLevelHttpRequest() {
                            @Override
                            public LowLevelHttpResponse execute() throws IOException {
                                return httpResponse;
                            }
                        })
                .build();
    }

    protected Storage authenticatedGCStorageInstance() {
        return StorageOptions.newBuilder()
                .setCredentials(getGoogleCredentials())
                .build().getService();
    }

    protected TestMessage getTestMessage(String id, Instant now) {
        long second = now.getEpochSecond();
        int nano = now.getNano();
        Timestamp createdDate = Timestamp.newBuilder().setSeconds(second).setNanos(nano).build();
        return TestMessage.newBuilder()
                .setOrderNumber("Order-No-" + id)
                .setOrderUrl("Order-Url-" + id)
                .setOrderDetails("Order-details-" + id)
                .setCreatedAt(createdDate)
                .build();
    }

    protected List<Record> getKafkaConsumerRecords(ColumnMapping columnMapping, Instant now, String topicName,
                                                   int partitionStart, long offsetStart, Clock clock, TestMessage...testMessages) throws InvalidProtocolBufferException {
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        ConsumerRecordConverter customConverter = new ConsumerRecordConverter(new RowMapper(columnMapping), protoParser, clock, ConfigFactory.create(AppConfig.class, System.getProperties()));
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        for (int i = 0; i < testMessages.length; i++) {
            consumerRecordsList.add(new ConsumerRecord<>(topicName, partitionStart + i, offsetStart + i, now.getEpochSecond(), TimestampType.CREATE_TIME,
                    0, 0, 1, null, testMessages[i].toByteArray()));
        }
        return customConverter.convert(consumerRecordsList);
    }
}
