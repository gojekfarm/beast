package com.gojek.beast.converter;

import com.gojek.beast.Status;
import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.gojek.beast.TestNestedRepeatedMessage;
import com.gojek.beast.config.ColumnMapping;
import com.gojek.beast.models.ConfigurationException;
import com.gojek.beast.util.ProtoUtil;
import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.api.client.util.DateTime;
import com.google.cloud.Date;
import com.google.protobuf.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class RowMapperTest {

    private Timestamp createdAt;
    private DynamicMessage dynamicMessage;
    private Instant now;
    private long nowMillis;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        now = Instant.now();
        createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestMessage testMessage = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(Status.COMPLETED)
                .setOrderDate(com.google.type.Date.newBuilder().setYear(1996).setMonth(11).setDay(21))
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
        fieldMappings.put("14", "order_date_field");

        Map<String, Object> fields = new RowMapper(fieldMappings).map(dynamicMessage);

        assertEquals("order-1", fields.get("order_number_field"));
        assertEquals("order-url", fields.get("order_url_field"));
        assertEquals("order-details", fields.get("order_details_field"));
        assertEquals(new DateTime(nowMillis), fields.get("created_at"));
        assertEquals("COMPLETED", fields.get("order_status"));
        assertEquals(Date.fromYearMonthDay(1996, 11, 21), fields.get("order_date_field"));
        assertEquals(fieldMappings.size(), fields.size());
    }

    @Test
    public void shouldParseDurationMessageSuccessfully() throws InvalidProtocolBufferException {
        ColumnMapping fieldMappings = new ColumnMapping();
        ColumnMapping durationMappings = new ColumnMapping();
        durationMappings.put("record_name", "duration");
        durationMappings.put("1", "seconds");
        durationMappings.put("2", "nanos");
        fieldMappings.put("1", "duration_id");
        fieldMappings.put("11", durationMappings);

        TestMessage message = ProtoUtil.generateTestMessage(now);
        ProtoParser messageProtoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(messageProtoParser.parse(message.toByteArray()));
        Map durationFields = (Map) fields.get("duration");
        assertEquals("order-1", fields.get("duration_id"));
        assertEquals((long) 1, durationFields.get("seconds"));
        assertEquals(1000000000, durationFields.get("nanos"));
    }

    @Test
    public void shouldParseNestedMessageSuccessfully() {
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
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            assertNestedMessage(msg, fields);
        });
    }

    @Test
    public void shouldParseRepeatedPrimitives() throws InvalidProtocolBufferException {
        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number");
        fieldMappings.put("12", "aliases");

        String orderNumber = "order-1";
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber(orderNumber)
                .setOrderUrl("order-url-1")
                .addAliases("alias1").addAliases("alias2")
                .build();

        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(protoParser.parse(message.toByteArray()));

        assertEquals(orderNumber, fields.get("order_number"));
        assertEquals(Arrays.asList("alias1", "alias2"), fields.get("aliases"));
    }

    @Test
    public void shouldParseRepeatedNestedMessages() throws InvalidProtocolBufferException {
        int number = 1234;
        TestMessage nested1 = ProtoUtil.generateTestMessage(now);
        TestMessage nested2 = ProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessage message = TestNestedRepeatedMessage.newBuilder()
                .setNumberField(number)
                .addRepeatedMessage(nested1)
                .addRepeatedMessage(nested2)
                .build();


        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("3", "number_field");
        fieldMappings.put("2", getTestMessageColumnMapping());


        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestNestedRepeatedMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(protoParser.parse(message.toByteArray()));

        assertEquals(number, fields.get("number_field"));
        List repeatedMessagesMap = (List) fields.get("msg");
        assertTestMessageFields((Map) repeatedMessagesMap.get(0), nested1);
        assertTestMessageFields((Map) repeatedMessagesMap.get(1), nested2);
    }

    @Test
    public void shouldParseRepeatedNestedMessagesIfRepeatedFieldsAreMissing() throws InvalidProtocolBufferException {
        int number = 1234;
        TestMessage nested1 = ProtoUtil.generateTestMessage(now);
        TestMessage nested2 = ProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessage message = TestNestedRepeatedMessage.newBuilder()
                .setNumberField(number)
                .build();


        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("3", "number_field");
        fieldMappings.put("2", getTestMessageColumnMapping());

        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestNestedRepeatedMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(protoParser.parse(message.toByteArray()));

        assertEquals(number, fields.get("number_field"));
        assertEquals(1, fields.size());
    }

    @Test
    public void shouldParseMapFields() throws InvalidProtocolBufferException {
        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url-1")
                .setOrderDetails("order-details-1")
                .putCurrentState("state_key_1", "state_value_1")
                .putCurrentState("state_key_2", "state_value_2")
                .build();

        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number_field");
        fieldMappings.put("2", "order_url_field");
        ColumnMapping currStateMapping = new ColumnMapping();
        currStateMapping.put("record_name", "current_state");
        currStateMapping.put("1", "key");
        currStateMapping.put("2", "value");
        fieldMappings.put("9", currStateMapping);

        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(protoParser.parse(message.toByteArray()));

        assertEquals(message.getOrderNumber(), fields.get("order_number_field"));
        assertEquals(message.getOrderUrl(), fields.get("order_url_field"));
        List repeatedStateMap = (List) fields.get("current_state");
        assertEquals("state_key_1", ((Map) repeatedStateMap.get(0)).get("key"));
        assertEquals("state_value_1", ((Map) repeatedStateMap.get(0)).get("value"));
        assertEquals("state_key_2", ((Map) repeatedStateMap.get(1)).get("key"));
        assertEquals("state_value_2", ((Map) repeatedStateMap.get(1)).get("value"));
    }

    @Test
    public void shouldMapStructFields() throws InvalidProtocolBufferException {
        ListValue.Builder builder = ListValue.newBuilder();
        ListValue listValue = builder
                .addValues(Value.newBuilder().setNumberValue(1).build())
                .addValues(Value.newBuilder().setNumberValue(2).build())
                .addValues(Value.newBuilder().setNumberValue(3).build())
                .build();
        Struct value = Struct.newBuilder()
                .putFields("number", Value.newBuilder().setNumberValue(123.45).build())
                .putFields("string", Value.newBuilder().setStringValue("string_val").build())
                .putFields("list", Value.newBuilder().setListValue(listValue).build())
                .putFields("boolean", Value.newBuilder().setBoolValue(true).build())
                .build();

        TestMessage message = TestMessage.newBuilder()
                .setOrderNumber("order-1")
                .setProperties(value)
                .build();

        ColumnMapping fieldMappings = new ColumnMapping();
        fieldMappings.put("1", "order_number_field");
        fieldMappings.put("13", "properties");

        ProtoParser protoParser = new ProtoParser(StencilClientFactory.getClient(), TestMessage.class.getName());
        Map<String, Object> fields = new RowMapper(fieldMappings).map(protoParser.parse(message.toByteArray()));

        assertEquals(message.getOrderNumber(), fields.get("order_number_field"));
        String expectedProperties = "{\"number\":123.45,\"string\":\"string_val\",\"list\":[1.0,2.0,3.0],\"boolean\":true}";
        assertEquals(expectedProperties, fields.get("properties"));
    }

    private void assertNestedMessage(TestNestedMessage msg, Map<String, Object> fields) {
        assertEquals(msg.getNestedId(), fields.get("nested_id"));
        Map nestedFields = (Map) fields.get("msg");
        assertNotNull(nestedFields);
        TestMessage message = msg.getSingleMessage();
        assertTestMessageFields(nestedFields, message);
    }

    private void assertTestMessageFields(Map nestedFields, TestMessage message) {
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
