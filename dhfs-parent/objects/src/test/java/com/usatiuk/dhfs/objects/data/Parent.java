package com.usatiuk.dhfs.objects.data;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public record Parent(JObjectKey key, String name) implements JData {
    public Parent withName(String name) {
        return new Parent(key, name);
    }
}