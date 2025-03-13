package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.IteratorStart;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

@FunctionalInterface
public interface IterProdFn<K extends Comparable<K>, V> extends AutoCloseableNoThrow {
    CloseableKvIterator<K, V> get(IteratorStart start, K key);

    @Override
    default void close() {
    }
}
