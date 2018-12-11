package com.gojek.beast.converter;

import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.ConfigurationException;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.parser.ProtoParser;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.api.client.util.DateTime;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class RowMapperTest {

    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;
    private Instant now;
    private long nowMillis;

    @Before
    public void setUp() throws ParseException {
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        now = Instant.now();
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setCreatedAt(createdAt)
                .build();
        dynamicMessage = protoParser.parse(testMessage.toByteArray());
        nowMillis = Instant.ofEpochSecond(now.getEpochSecond(), now.getNano()).toEpochMilli();
    }

    @Test
    public void shouldReturnFieldsInColumnMapping() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number_field");
        fieldMappings.put("4", "created_at");

        Map<String, Object> fields = new RowMapper(fieldMappings).map(dynamicMessage);

        assertEquals("order-1", fields.get("order_number_field"));
        assertEquals(new DateTime(nowMillis), fields.get("created_at"));
        assertEquals(fieldMappings.size(), fields.size());
    }

    @Test
    public void shouldReturnNullWhenIndexNotPresent() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("10", "some_column_in_bq");

        Map<String, Object> fields = new RowMapper(fieldMappings).map(dynamicMessage);

        assertNull(fields.get("some_column_in_bq"));
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowExceptionWhenConfigNotPresent() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("10", "some_column_in_bq");

        new RowMapper(null).map(dynamicMessage);
    }
}
