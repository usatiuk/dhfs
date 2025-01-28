package com.usatiuk.dhfs.objects;

import java.io.Serializable;
import java.util.UUID;

public record PeerId(UUID id) implements Serializable {
    public static PeerId of(UUID id) {
        return new PeerId(id);
    }

    public static PeerId of(String id) {
        return new PeerId(UUID.fromString(id));
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public JObjectKey toJObjectKey() {
        return JObjectKey.of(id.toString());
    }
}
