package com.usatiuk.dhfs;

import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.JObjectKeyImpl;

import java.io.Serializable;

public record PeerId(JObjectKey id) implements Serializable, Comparable<PeerId> {
    public static PeerId of(String id) {
        return new PeerId(JObjectKey.of(id));
    }

    public static PeerId of(JObjectKey id) {
        return new PeerId(id);
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public JObjectKey toJObjectKey() {
        return id();
    }

    @Override
    public int compareTo(PeerId o) {
        return id.compareTo(o.id);
    }
}
