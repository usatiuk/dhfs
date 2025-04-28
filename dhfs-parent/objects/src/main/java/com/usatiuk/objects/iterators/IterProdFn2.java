package com.usatiuk.objects.iterators;

import java.util.stream.Stream;

@FunctionalInterface
public interface IterProdFn2<K extends Comparable<K>, V> {
    Stream<CloseableKvIterator<K, MaybeTombstone<V>>> get(IteratorStart start, K key);
}
