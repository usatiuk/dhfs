package com.usatiuk.objects.snapshot;

import com.usatiuk.objects.iterators.CloseableKvIterator;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.iterators.MaybeTombstone;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public interface Snapshot<K extends Comparable<K>, V> extends AutoCloseable {
    List<CloseableKvIterator<K, MaybeTombstone<V>>> getIterator(IteratorStart start, K key);

    @Nonnull
    Optional<V> readObject(K name);

    long id();

    @Override
    void close();

}
