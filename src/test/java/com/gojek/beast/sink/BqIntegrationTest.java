package com.gojek.beast.sink;

import com.gojek.beast.models.Records;
import com.gojek.beast.models.Status;
import com.gojek.beast.sink.bq.BqSink;
import com.gojek.beast.models.Record;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

@Ignore
public class BqIntegrationTest {
    @Mock
    private MockLowLevelHttpResponse response;
    //{0=[BigQueryError{reason=invalid, location=age, message=no such field.}]}

    public BigQuery authenticatedBQ() {
        GoogleCredentials credentials = null;
        File credentialsPath = new File("credentials.json  # Replace, using a regex");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .build().getService();
    }

    public MockHttpTransport getTransporter(final MockLowLevelHttpResponse httpResponse) {
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

    @Test
    public void shouldPushMessagesToBq() {
        BigQuery bq = authenticatedBQ();
        TableId tableId = TableId.of("bqsinktest", "users");
        BqSink bqSink = new BqSink(bq, tableId);


        HashMap<String, Object> columns = new HashMap<>();
        columns.put("name", "alice");
        columns.put("aga", 25);
        columns.put("location", 25123);
        columns.put("created_at", new DateTime(new Date()));

        Status push = bqSink.push(new Records(Arrays.asList(new Record(columns))));

        assertTrue(push.isSuccess());
    }
}
