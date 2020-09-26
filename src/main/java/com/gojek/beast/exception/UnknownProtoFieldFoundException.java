package com.gojek.beast.exception;

import com.google.protobuf.UnknownFieldSet;

import java.util.Map;

public class UnknownProtoFieldFoundException extends RuntimeException {
    public UnknownProtoFieldFoundException(Map<Integer, UnknownFieldSet.Field> unknownFields) {
        super(String.format("%d unknown fields found in proto, either update mapped protobuf or disable FAIL_ON_UNKNOWN_FIELDS", unknownFields.size()));
    }
}
