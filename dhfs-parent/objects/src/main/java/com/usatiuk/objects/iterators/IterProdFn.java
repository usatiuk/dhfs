package com.usatiuk.objects.iterators;

import java.util.stream.Stream;

@FunctionalInterface
public interface IterProdFn<K extends Comparable<K>, V> {
    CloseableKvIterator<K, V> get(IteratorStart start, K key);

    default Stream<CloseableKvIterator<K, MaybeTombstone<V>>> getFlat(IteratorStart start, K key) {
        return Stream.of(new MappingKvIterator<>(get(start, key), Data::new));
    }
}
