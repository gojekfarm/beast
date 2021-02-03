package com.gojek.beast.protomapping;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import lombok.extern.slf4j.Slf4j;

/**
 * Try to convert raw proto bytes to some meaningful representation that is good enough for debug.
 * */
@Slf4j
public class UnknownProtoFields {
    public static String toString(byte[] message) {
        String convertedFields = "";
        try {
            convertedFields = UnknownFieldSet.parseFrom(message).toString();
        } catch (InvalidProtocolBufferException e) {
            log.warn("invalid byte representation of a protobuf message");
        }
        return convertedFields;
    }
}
