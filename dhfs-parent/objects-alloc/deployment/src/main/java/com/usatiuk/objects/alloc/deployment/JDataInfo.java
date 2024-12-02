package com.usatiuk.objects.alloc.deployment;

import org.jboss.jandex.ClassInfo;

import java.util.Map;

public record JDataInfo(ClassInfo klass, Map<String, JDataFieldInfo> fields) {
}
