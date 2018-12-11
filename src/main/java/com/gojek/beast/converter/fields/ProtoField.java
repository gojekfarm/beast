package com.gojek.beast.converter.fields;

public interface ProtoField {

    Object getValue();

    boolean matches();
}
