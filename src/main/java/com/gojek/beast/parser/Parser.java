package com.gojek.beast.parser;

import com.gojek.beast.models.ParseException;
import com.google.protobuf.DynamicMessage;

public interface Parser {
    DynamicMessage parse(byte[] data) throws ParseException;
}

