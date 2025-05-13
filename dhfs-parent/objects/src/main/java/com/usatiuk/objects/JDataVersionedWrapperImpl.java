package com.usatiuk.objects;

import jakarta.annotation.Nonnull;

import java.io.Serializable;

/**
 * Simple wrapper for an already-existing JData object with a version.
 */
public record JDataVersionedWrapperImpl(@Nonnull JData data,
                                        long version) implements Serializable, JDataVersionedWrapper {
    @Override
    public int estimateSize() {
        return data().estimateSize();
    }
}
