package com.usatiuk.dhfs.objects;

import java.io.Serializable;

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
}
