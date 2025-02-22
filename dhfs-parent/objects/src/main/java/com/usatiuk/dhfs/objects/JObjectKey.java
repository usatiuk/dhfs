package com.usatiuk.dhfs.objects;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public record JObjectKey(String name) implements Serializable, Comparable<JObjectKey> {
    public static JObjectKey of(String name) {
        return new JObjectKey(name);
    }

    @Override
    public int compareTo(JObjectKey o) {
        return name.compareTo(o.name);
    }

    @Override
    public String toString() {
        return name;
    }

    public byte[] bytes() {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    public static JObjectKey fromBytes(byte[] bytes) {
        return new JObjectKey(new String(bytes, StandardCharsets.UTF_8));
    }
}
