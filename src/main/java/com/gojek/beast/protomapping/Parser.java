package com.gojek.beast.protomapping;

import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;

import java.util.Map;

public class Parser {
    private Stats statsClient = Stats.client();

    public ProtoField parseFields(ProtoField protoField, String protoSchema, StencilClient stencilClient) {
        Map<String, Descriptors.Descriptor> allDescriptors = stencilClient.getAll();
        Descriptors.Descriptor currentProto = allDescriptors.get(protoSchema);
        if (currentProto == null) {
            throw new ProtoNotFoundException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                Descriptors.Descriptor nestedDP = allDescriptors.get(String.format("com%s", field.toProto().getTypeName()));
                if (nestedDP == null) {
                    statsClient.increment(String.format("proto.notfound.errors,proto=%s", field.getFullName()));
                    throw new ProtoNotFoundException("No Proto found for class " + field.getFullName());
                } else {
                    fieldModel = parseFields(fieldModel, String.format("com%s", field.toProto().getTypeName()), stencilClient);
                }
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
