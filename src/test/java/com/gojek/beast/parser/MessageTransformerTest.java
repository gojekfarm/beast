package com.gojek.beast.parser;

import com.gojek.beast.TestMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.ConfigurationException;
import com.gojek.beast.models.ParseException;
import com.gojek.de.stencil.StencilClientFactory;
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
public class MessageTransformerTest {

    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;

    @Before
    public void setUp() throws ParseException {
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        Instant time = Instant.now();
        createdAt = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setCreatedAt(createdAt)
                .build();
        dynamicMessage = protoParser.parse(testMessage.toByteArray());
    }

    @Test
    public void shouldReturnFieldsInColumnMapping() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number_field");
        fieldMappings.put("4", "created_at");

        Map<String, Object> fields = new MessageTransformer(fieldMappings).getFields(dynamicMessage);

        assertEquals("order-1", fields.get("order_number_field"));
        assertEquals(createdAt, fields.get("created_at"));
        assertEquals(fieldMappings.size(), fields.size());
    }

    @Test
    public void shouldReturnNullWhenIndexNotPresent() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("10", "some_column_in_bq");

        Map<String, Object> fields = new MessageTransformer(fieldMappings).getFields(dynamicMessage);

        assertNull(fields.get("some_column_in_bq"));
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowExceptionWhenConfigNotPresent() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("10", "some_column_in_bq");

        new MessageTransformer(null).getFields(dynamicMessage);
    }
}
