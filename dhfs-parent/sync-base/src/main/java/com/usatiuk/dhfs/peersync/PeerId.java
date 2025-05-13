package com.usatiuk.dhfs.peersync;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;

/**
 * Represents a peer ID
 * Type-safe wrapper for JObjectKey
 * @param id the ID of the peer (as a JObjectKey)
 */
public record PeerId(JObjectKey id) implements Serializable, Comparable<PeerId> {
    public static PeerId of(String id) {
        return new PeerId(JObjectKey.of(id));
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public JObjectKey toJObjectKey() {
        return JObjectKey.of(id.toString());
    }

    @Override
    public int compareTo(PeerId o) {
        return id.compareTo(o.id);
    }
}
