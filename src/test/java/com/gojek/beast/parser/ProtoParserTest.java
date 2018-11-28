package com.gojek.beast.parser;

import com.gojek.beast.TestMessage;
import com.gojek.beast.TestNestedMessage;
import com.gojek.beast.models.ConfigurationException;
import com.gojek.beast.models.ParseException;
import com.gojek.de.stencil.StencilClient;
import com.gojek.de.stencil.StencilClientFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.gradle.internal.impldep.org.testng.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.gradle.internal.impldep.org.testng.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ProtoParserTest {

    private ProtoParser testMessageParser;
    private StencilClient stencilClient;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        stencilClient = StencilClientFactory.getClient();
        testMessageParser = new ProtoParser(stencilClient, TestMessage.class.getName());
    }

    @Test
    public void shouldParseTestMessage() {
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order").build();
        DynamicMessage dynamicMessage = testMessageParser.parse(testMessage.toByteArray());

        Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().getFields().get(0);

        assertEquals(dynamicMessage.getField(fieldDescriptor), "order");
        assertEquals(dynamicMessage.toString(), testMessage.toString());
    }

    @Test
    public void shouldNotParseRandomLogMessage() {
        TestNestedMessage protoMessage = TestNestedMessage.newBuilder().build();
        DynamicMessage message = testMessageParser.parse(protoMessage.toByteArray());

        assertNotEquals(message.toByteArray(), "random".getBytes());
        assertEquals(message.getAllFields().size(), 0);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldFailWhenNotAbleToFindTheProtoClass() {
        ProtoParser protoParser = new ProtoParser(stencilClient, "invalid_class_name");

        protoParser.parse("".getBytes());

        Assert.fail("Expected to get an exception");
    }

    @Test(expected = ParseException.class)
    public void shouldFailForInvalidProtoMessage() {
        ProtoParser protoParser = new ProtoParser(stencilClient, TestMessage.class.getName());
        protoParser.parse("invalid".getBytes());
    }
}
