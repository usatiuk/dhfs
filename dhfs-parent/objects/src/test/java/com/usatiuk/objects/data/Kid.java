package com.usatiuk.objects.data;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public record Kid(JObjectKey key, String name) implements JData {
    public Kid withName(String name) {
        return new Kid(key, name);
    }
}