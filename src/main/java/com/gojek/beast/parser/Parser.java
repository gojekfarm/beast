package com.gojek.beast.parser;

import com.google.protobuf.DynamicMessage;

public interface Parser {
    DynamicMessage parse(byte[] data) throws ParseException;
}

class ParseException extends RuntimeException {
    ParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
