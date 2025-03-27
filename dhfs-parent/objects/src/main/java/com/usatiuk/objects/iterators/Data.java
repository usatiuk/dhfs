package com.usatiuk.objects.iterators;

import java.util.Optional;

public record Data<V>(V value) implements MaybeTombstone<V> {
    @Override
    public Optional<V> opt() {
        return Optional.of(value);
    }
}
