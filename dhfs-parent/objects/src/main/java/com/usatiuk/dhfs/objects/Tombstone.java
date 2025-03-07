package com.usatiuk.dhfs.objects;

import java.util.Optional;

public record Tombstone<V>() implements MaybeTombstone<V> {
    @Override
    public Optional<V> opt() {
        return Optional.empty();
    }
}
