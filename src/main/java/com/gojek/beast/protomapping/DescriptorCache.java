package com.gojek.beast.protomapping;

import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;

import java.util.Map;

public class DescriptorCache {
    public Descriptors.Descriptor fetch(StencilClient stencilClient, String protoName) {
        Map<String, Descriptors.Descriptor> allDescriptors = stencilClient.getAll();
        Map<String, String> typeNameToPackageNameMap = stencilClient.getTypeNameToPackageNameMap();
        if (allDescriptors.get(protoName) != null) {
            return allDescriptors.get(protoName);
        }
        String packageName = typeNameToPackageNameMap.get(protoName);
        if (packageName == null) {
            return null;
        }
        return allDescriptors.get(packageName);
    }
}
