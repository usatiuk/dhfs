package com.usatiuk.dhfs.objects;

import jakarta.annotation.Nonnull;

import java.io.Serializable;

public record JDataVersionedWrapper(@Nonnull JData data, long version) implements Serializable {
    public JDataVersionedWrapper withVersion(long version) {
        return new JDataVersionedWrapper(data, version);
    }
}
