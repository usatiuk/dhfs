package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.utils.AutoCloseableNoThrow;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseableNoThrow {
    CloseableKvIterator<K, V> getIterator(IteratorStart start, K key);

    @Nonnull
    Optional<V> readObject(K name);

    long id();
}
