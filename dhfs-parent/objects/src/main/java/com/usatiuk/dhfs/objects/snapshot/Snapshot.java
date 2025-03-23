package com.usatiuk.dhfs.objects.snapshot;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.iterators.CloseableKvIterator;
import com.usatiuk.dhfs.objects.iterators.IteratorStart;
import com.usatiuk.dhfs.utils.AutoCloseableNoThrow;

import javax.annotation.Nonnull;
import java.util.Optional;

public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseableNoThrow {
    CloseableKvIterator<K, V> getIterator(IteratorStart start, JObjectKey key);

    @Nonnull
    Optional<V> readObject(K name);

    long id();
}
