package com.usatiuk.objects.snapshot;

import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;
import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IterProdFn;
import com.usatiuk.objects.iterators.IteratorStart;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseableNoThrow {
    IterProdFn<K, V> getIterator();

    default CloseableKvIterator<K, V> getIterator(IteratorStart start, K key) {
        return getIterator().get(start, key);
    }

    @Nonnull
    Optional<V> readObject(K name);

    long id();
}
