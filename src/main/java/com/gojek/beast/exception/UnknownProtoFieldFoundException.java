package com.gojek.beast.exception;

public class UnknownProtoFieldFoundException extends RuntimeException {
    public UnknownProtoFieldFoundException(String serialisedProtoMessage) {
        super(String.format("some unknown fields found, please check the published message, update mapped protobuf or disable FAIL_ON_UNKNOWN_FIELDS, full proto message : %s", serialisedProtoMessage));
    }
}
