package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;

@FunctionalInterface
public interface IterProdFn<K extends Comparable<K>, V> {
    CloseableKvIterator<K, V> get(IteratorStart start, K key);
}
