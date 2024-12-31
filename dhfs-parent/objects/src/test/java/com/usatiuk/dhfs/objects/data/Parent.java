package com.usatiuk.dhfs.objects.data;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
import lombok.Builder;

@Builder(toBuilder = true)
public record Parent(JObjectKey key, String name) implements JData {
}