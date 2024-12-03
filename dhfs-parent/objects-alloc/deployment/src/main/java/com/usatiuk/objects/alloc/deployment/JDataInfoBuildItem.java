package com.usatiuk.objects.alloc.deployment;

import io.quarkus.builder.item.MultiBuildItem;
import org.jboss.jandex.ClassInfo;

import java.util.Map;

public final class JDataInfoBuildItem extends MultiBuildItem {
    public final ClassInfo klass;
    public final Map<String, JDataFieldInfo> fields;

    public JDataInfoBuildItem(ClassInfo klass, Map<String, JDataFieldInfo> fields) {
        this.klass = klass;
        this.fields = fields;
    }
}
