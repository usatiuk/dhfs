package com.usatiuk.dhfs.objects.iterators;

@FunctionalInterface
public interface IterProdFn<K extends Comparable<K>, V> {
    CloseableKvIterator<K, V> get(IteratorStart start, K key);
}
