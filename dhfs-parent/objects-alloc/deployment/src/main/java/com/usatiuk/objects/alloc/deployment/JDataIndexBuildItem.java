package com.usatiuk.objects.alloc.deployment;

import io.quarkus.builder.item.MultiBuildItem;
import org.jboss.jandex.ClassInfo;

public final class JDataIndexBuildItem extends MultiBuildItem {
    public final ClassInfo jData;

    public JDataIndexBuildItem(ClassInfo jData) {
        this.jData = jData;
    }
}
