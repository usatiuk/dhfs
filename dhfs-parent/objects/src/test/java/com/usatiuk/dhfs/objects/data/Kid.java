package com.usatiuk.dhfs.objects.data;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public record Kid(JObjectKey key, String name) implements JData {
    public Kid withName(String name) {
        return new Kid(key, name);
    }
}