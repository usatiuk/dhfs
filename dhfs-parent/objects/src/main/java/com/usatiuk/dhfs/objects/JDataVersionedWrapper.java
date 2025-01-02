package com.usatiuk.dhfs.objects;

import jakarta.annotation.Nonnull;

import java.io.Serializable;

public record JDataVersionedWrapper<T extends JData>(@Nonnull T data, long version) implements Serializable {
    public JDataVersionedWrapper<T> withVersion(long version) {
        return new JDataVersionedWrapper<>(data, version);
    }
}
