package com.usatiuk.objects.data;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public record Parent(JObjectKey key, String name) implements JData {
    public Parent withName(String name) {
        return new Parent(key, name);
    }
}