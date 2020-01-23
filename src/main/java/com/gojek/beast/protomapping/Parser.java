package com.gojek.beast.protomapping;

import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.stats.Stats;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;

public class Parser {
    private final Stats statsClient = Stats.client();
    private final DescriptorCache descriptorCache = new DescriptorCache();

    public ProtoField parseFields(ProtoField protoField, String protoSchema, StencilClient stencilClient) {
        Descriptors.Descriptor currentProto = descriptorCache.fetch(stencilClient, protoSchema);
        if (currentProto == null) {
            throw new ProtoNotFoundException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                Descriptors.Descriptor nestedDP = descriptorCache.fetch(stencilClient, protoSchema);
                if (nestedDP == null) {
                    statsClient.increment(String.format("proto.notfound.errors,proto=%s", field.getFullName()));
                    throw new ProtoNotFoundException("No Proto found for class " + field.getFullName());
                } else {
                    fieldModel = parseFields(fieldModel, field.toProto().getTypeName(), stencilClient);
                }
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
