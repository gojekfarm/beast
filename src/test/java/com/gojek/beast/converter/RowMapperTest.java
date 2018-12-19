package com.gojek.beast.converter;

import com.gojek.beast.Status;
import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.ConfigurationException;
import com.gojek.beast.models.ParseException;
import com.gojek.beast.parser.ProtoParser;
import com.gojek.beast.util.ProtoUtil;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.api.client.util.DateTime;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(Status.COMPLETED)
                .build();
        dynamicMessage = protoParser.parse(testMessage.toByteArray());
        nowMillis = Instant.ofEpochSecond(now.getEpochSecond(), now.getNano()).toEpochMilli();
    }

    @Test
    public void shouldReturnFieldsInColumnMapping() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number_field");
        fieldMappings.put("2", "order_url_field");
        fieldMappings.put("3", "order_details_field");
        fieldMappings.put("4", "created_at");
        fieldMappings.put("5", "order_status");

        Map<String, Object> fields = new RowMapper(fieldMappings).map(dynamicMessage);

        assertEquals("order-1", fields.get("order_number_field"));
        assertEquals("order-url", fields.get("order_url_field"));
        assertEquals("order-details", fields.get("order_details_field"));
        assertEquals(new DateTime(nowMillis), fields.get("created_at"));
        assertEquals("COMPLETED", fields.get("order_status"));
        assertEquals(fieldMappings.size(), fields.size());
    }

    @Test
    public void shouldParseNestedMessageSuccessfully() throws ParseException {
        ColumnMapping fieldMappings = new ColumnMapping();
        ColumnMapping nestedMappings = getTestMessageColumnMapping();
        fieldMappings.put("1", "nested_id");
        fieldMappings.put("2", nestedMappings);

        TestMessage message1 = ProtoUtil.generateTestMessage(now);
        TestMessage message2 = ProtoUtil.generateTestMessage(now);

        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestNestedMessage.class.getName());
        TestNestedMessage nestedMessage1 = ProtoUtil.generateTestNestedMessage("nested-message-1", message1);
        TestNestedMessage nestedMessage2 = ProtoUtil.generateTestNestedMessage("nested-message-2", message2);
        Arrays.asList(nestedMessage1, nestedMessage2).forEach(msg -> {
            Map<String, Object> fields = null;
            try {
                fields = new RowMapper(fieldMappings).map(protoParser.parse(msg.toByteArray()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            assertNestedMessage(msg, fields);
        });
    }

    private void assertNestedMessage(TestNestedMessage msg, Map<String, Object> fields) {
        assertEquals(msg.getNestedId(), fields.get("nested_id"));
        Map nestedFields = (Map) fields.get("msg");
        assertNotNull(nestedFields);
        TestMessage message = msg.getSingleMessage();
        assertEquals(message.getOrderNumber(), nestedFields.get("order_number_field"));
        assertEquals(message.getOrderUrl(), nestedFields.get("order_url_field"));
        assertEquals(message.getOrderDetails(), nestedFields.get("order_details_field"));
        assertEquals(new DateTime(nowMillis), nestedFields.get("created_at_field"));
        assertEquals(message.getStatus().toString(), nestedFields.get("status_field"));
    }

    private ColumnMapping getTestMessageColumnMapping() {
        ColumnMapping nestedMappings = new ColumnMapping();
        nestedMappings.put("record_name", "msg");
        nestedMappings.put("1", "order_number_field");
        nestedMappings.put("2", "order_url_field");
        nestedMappings.put("3", "order_details_field");
        nestedMappings.put("4", "created_at_field");
        nestedMappings.put("5", "status_field");
        return nestedMappings;
    }

    @Test()
    public void shouldReturnNullWhenIndexNotPresent() {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("100", "some_column_in_bq");

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
