package com.gojek.beast.models;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.DescriptorProtos;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProtoField {
    private String name;
    private String typeName;
    private DescriptorProtos.FieldDescriptorProto.Type type;
    private DescriptorProtos.FieldDescriptorProto.Label label;
    private List<ProtoField> fields;
    private int index;
    private DescriptorProtos.FieldDescriptorProto fieldProto;

    public ProtoField() {
        this.fields = new ArrayList<>();
    }

    public ProtoField(DescriptorProtos.FieldDescriptorProto f) {
        this.fieldProto = f;
        this.name = f.getName();
        this.type = f.getType();
        this.label = f.getLabel();
        this.index = f.getNumber();
        this.fields = new ArrayList<>();
        this.typeName = f.getTypeName();
    }

    @VisibleForTesting
    public ProtoField(String name, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        this.name = name;
        this.type = type;
        this.label = label;
    }

    @VisibleForTesting
    public ProtoField(List<ProtoField> subFields) {
        this.fields = subFields;
    }

    @VisibleForTesting
    public ProtoField(String name, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label, List<ProtoField> fields) {
        this.name = name;
        this.type = type;
        this.label = label;
        this.fields = fields;
    }

    @VisibleForTesting
    public ProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
        this.label = label;
    }

    @VisibleForTesting
    public ProtoField(String name, String typeName, DescriptorProtos.FieldDescriptorProto.Type type, DescriptorProtos.FieldDescriptorProto.Label label, List<ProtoField> fields) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
        this.label = label;
        this.fields = fields;
    }

    @VisibleForTesting
    public ProtoField(String name, int index) {
        this.name = name;
        this.index = index;
    }

    @VisibleForTesting
    public ProtoField(String name, int index, List<ProtoField> fields) {
        this.name = name;
        this.fields = fields;
        this.index = index;
    }

    public boolean isNested() {
        return this.fields != null && fieldProto == null ? this.fields.size() != 0
                : fieldProto != null && fieldProto.getAllFields().size() != 0
                &&
                (
                        type == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE
                                && !fieldProto.getTypeName().equals(".google.protobuf.Timestamp")
                                && !fieldProto.getTypeName().equals(".google.protobuf.Struct")
                                && !fieldProto.getTypeName().equals(".google.type.Date")
                );
    }

    public void addField(ProtoField field) {
        this.fields.add(field);
    }

    @Override
    public String toString() {
        return "{"
                + "name='" + name + '\''
                + ", type=" + type
                + ", len=" + fields.size()
                + ", nested=" + Arrays.toString(fields.toArray())
                + '}';
    }

    public List<ProtoField> getFields() {
        return fields;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public DescriptorProtos.FieldDescriptorProto.Label getLabel() {
        return label;
    }

    public DescriptorProtos.FieldDescriptorProto.Type getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }
}
