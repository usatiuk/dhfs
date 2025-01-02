package com.usatiuk.dhfs.objects.data;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import lombok.Builder;

@Builder(toBuilder = true)
public record Parent(JObjectKey key, String name) implements JData {
}