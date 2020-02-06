package com.gojek.beast.protomapping;

import com.gojek.beast.exception.ProtoNotFoundException;
import com.gojek.beast.models.ProtoField;
import com.gojek.beast.stats.Stats;
import com.google.protobuf.Descriptors;

import java.util.Map;

public class Parser {
    private final Stats statsClient = Stats.client();
    private final DescriptorCache descriptorCache = new DescriptorCache();

    public ProtoField parseFields(ProtoField protoField, String protoSchema, Map<String, Descriptors.Descriptor> allDescriptors,
                                  Map<String, String> typeNameToPackageNameMap) {
        Descriptors.Descriptor currentProto = descriptorCache.fetch(allDescriptors, typeNameToPackageNameMap, protoSchema);
        if (currentProto == null) {
            statsClient.increment(String.format("proto.notfound.errors,proto=%s", protoSchema));
            throw new ProtoNotFoundException("No Proto found for class " + protoSchema);
        }
        for (Descriptors.FieldDescriptor field : currentProto.getFields()) {
            ProtoField fieldModel = new ProtoField(field.toProto());
            if (fieldModel.isNested()) {
                fieldModel = parseFields(fieldModel, field.toProto().getTypeName(), allDescriptors, typeNameToPackageNameMap);
            }
            protoField.addField(fieldModel);
        }
        return protoField;
    }
}
